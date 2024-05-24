from ...SafeConsumer.serviceB import ServiceB
from ....KafkaTestTool.kafkaTestTool import KafkaTestTool
from ..assertions import Assertions

from runtime import Runtime
from reflection import Reflection
from time import Time
from console import Console
from database import Database
from json_utils import JsonUtils

interface ServiceBFailureInterface {
    RequestResponse:
        /// @BeforeAll
        connect_to_db(void)(void),

        /// @BeforeAll
        init_default_testcase(void)(void),

        /// @Test
        Message_is_delivered_if_no_errors_occur(void)(void) throws AssertionError(string),
        
        /// @Test
        Message_is_delivered_if_service_b_crashes_on_receiving(void)(void) throws AssertionError(string),

        /// @Test
        Message_is_delivered_if_service_b_crashes_after_updating_local(void)(void) throws AssertionError(string),

        /// @Test
        B_only_handles_one_if_two_messages_with_same_mid_are_recieved(void)(void) throws AssertionError(string),
        
        /// @Test
        Message_is_delivered_if_IR_throws_before_starting_a_transaction(void)(void) throws AssertionError(string),
        
        /// @Test
        Message_is_delivered_if_IR_throws_after_deleting_message_from_inbox(void)(void) throws AssertionError(string),
        
        /// @Test
        Message_is_delivered_if_IW_throws_before_inserting_the_message_into_the_inbox(void)(void) throws AssertionError(string),

        /// @Test
        Message_is_delivered_if_IW_throws_after_inserting_it_into_the_inbox_but_before_responding(void)(void) throws AssertionError(string),

        /// @Test
        Message_is_delivered_if_MRS_throws_immediatly_after_finding_a_message(void)(void) throws AssertionError(string), 
        
        /// @Test
        Message_is_delivered_if_MRS_throws_after_notify_inbox_but_before_committing_to_kafka(void)(void) throws AssertionError(string),

        /// @AfterEach
        clear_tables(void)(void)
}

service ServiceBFailure{
    execution: sequential

    inputPort TestInterface {
        Location: "local"
        Interfaces: ServiceBFailureInterface 
    }
    embed KafkaTestTool as KafkaTestTool
    embed Assertions as Assertions
    embed ServiceB as ServiceB

    embed JsonUtils as JsonUtils
    embed Database as Database
    embed Time as Time
    embed Runtime as Runtime
    embed Console as Console
    embed Reflection as Reflection

    main
    {
        //  -------------- Setup and teardown tests:
        [connect_to_db()(){
            // Connect to db
            println@Console("\n\n------------------ Connecting to Database ------------------")()
            with ( connectionInfo ){
                .username = "postgres"
                .password = "example"
                .database = "service-b-db"
                .driver = "postgresql"
                .host = ""
            }
            connect@Database(connectionInfo)()
            
            // Setup the Kafka test tool:
            println@Console("\n\n------------------ Starting Kafka Test Tool ------------------")()
            setupTestConsumer@KafkaTestTool({
                .bootstrapServer = "localhost:29092"      // As seen in .MRS for serviceB
                .topic = "a-out"                          // ^^ Same
            })()

            setupTestProducer@KafkaTestTool({
                .bootstrapServer = "localhost:29092"
            })()

            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM users WHERE true;")()
            update@Database("DELETE FROM inbox WHERE true;")()
        }]

        [clear_tables()(){
            // Sleep here to avoid having multiple tests manipulating the same dabatase file
            sleep@Time(1000)()
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM users WHERE true;")()
            update@Database("DELETE FROM inbox WHERE true;")()
        }]

        [init_default_testcase()(){
            global.DefaultTestCase << {
                .serviceB << {
                    .throw_on_message_received = false
                    .throw_after_local_update = false
                }
                .inboxWriterTests << {
                    .throw_before_updating_inbox = false
                    .throw_after_updating_inbox_but_before_response = false
                }
                .inboxReaderTests << {
                    .throw_before_begin_tx = false
                    .throw_after_update_inbox = false
                }
                .mrsTests << {
                    .throw_after_message_found = false
                    .throw_after_notify_inbox_but_before_commit_to_kafka = false
                }
            }
        }]

        //  -------------- Service B Tests:
        [Message_is_delivered_if_no_errors_occur()(){
            // Arrange
            testCase << global.DefaultTestCase

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }
            
            getJsonString@JsonUtils({.username = "user1"})(params)
            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the system to work when no errors happen
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 user but found '" + #queryResult.row[0] + "'"
            })()

            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]

        [Message_is_delivered_if_service_b_crashes_on_receiving()(){
            /* 
             *  Since the InboxReader will simply abort the transaction if something occurs in serviceB,
             *  the message should be read again in the next iterarion of the loop 
             */

            // Arrange
            testCase << global.DefaultTestCase
            testCase.serviceB.throw_on_message_received = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the system to work when no errors happen
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row[0] + "'"
            })()

            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()

        }]
        
        [Message_is_delivered_if_service_b_crashes_after_updating_local()(){
            /* 
             *  Since the InboxReader will simply abort the transaction if something occurs in serviceB,
             *  the message should be read again in the next iterarion of the loop 
             */

            // Arrange
            testCase << global.DefaultTestCase
            testCase.serviceB.throw_after_local_update = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )

            // We expect both messages to be present in local storage at B
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row + "'"
            })()
        }]

        [B_only_handles_one_if_two_messages_with_same_mid_are_recieved()(){
            /*
             *  This can occur if the MFS in ServiceA crashes after having forwarded a message, but before deleting it from the DB.
             *  The uniqueness constraint on the Inbox table should mean that this message is only received once
             */


            // Arrange
            testCase << global.DefaultTestCase

            setupConsumerTests@ServiceB(testCase)()
            with (message1){
                .topic = "a-out"
                .key = "react"
            }

            with (message2){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message1.value)

            getJsonString@JsonUtils({.username = "user2"})(params)
            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message2.value)

            // Act
            send@KafkaTestTool(message1)()
            send@KafkaTestTool(message2)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )

            // We expect only one message to be recieved, as the mid is unique
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row + "'"
            })()

            // We even expect the messasges to have arrived in causal ordering
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]

        // -------------- InboxReader Tests:
        [Message_is_delivered_if_IR_throws_before_starting_a_transaction()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.inboxReaderTests.throw_before_begin_tx = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the message (m) to be delivered
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row[0] + "'"
            })()

            // We expect this message to be the one we just sent
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]

        [Message_is_delivered_if_IR_throws_after_deleting_message_from_inbox()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.inboxReaderTests.throw_after_update_inbox = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the message (m) to be delivered
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row[0] + "'"
            })()

            // We expect this message to be the one we just sent
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]


        //-------------- InboxWriter Tests:
        [Message_is_delivered_if_IW_throws_before_inserting_the_message_into_the_inbox()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.inboxWriterTests.throw_before_updating_inbox = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the message (m) to be delivered
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row[0] + "'"
            })()

            // We expect this message to be the one we just sent
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]

        [Message_is_delivered_if_IW_throws_after_inserting_it_into_the_inbox_but_before_responding()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.inboxWriterTests.throw_after_updating_inbox_but_before_response = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the message (m) to be delivered
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row[0] + "'"
            })()

            // We expect this message to be the one we just sent
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]

        // -------------- MRS Tests:
        [Message_is_delivered_if_MRS_throws_immediatly_after_finding_a_message()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.mrsTests.throw_after_message_found = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)

            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the message (m) to be delivered, as the offset for m is not committed to Kafka
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row[0] + "'"
            })()

            // We expect this message to be the one we just sent
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]

        [Message_is_delivered_if_MRS_throws_after_notify_inbox_but_before_committing_to_kafka()(){
            /*
            In the case of this test, the MRS first throws an exception after it has notified the inbox. This means that the inbox, and
            thus service B still receives and acts upon the message. The message is therefore entered into the Inbox table.
            Since the offset was never committed to kafka, however, when the MRS starts up again, it will still find the message next
            Time it polls kafka. When this happens, it will again call receiveKafka at the inbox service, but since the message is 
            unique in the inbox table, and the mid has not changed, the inbox will simply tell the MRS that 
            'hey, i've already handled this message. Please commit it again'
            */
            // Arrange
            testCase << global.DefaultTestCase
            testCase.mrsTests.throw_after_message_found = true

            setupConsumerTests@ServiceB(testCase)()
            with (message){
                .topic = "a-out"
                .key = "react"
            }

            getJsonString@JsonUtils({.username = "user1"})(params)
            getJsonString@JsonUtils({
                .mid = 0
                .parameters = params
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(1000)()

            // Assert
            query@Database( "Select * from users;" )( queryResult )
            
            // We expect the message (m) to be delivered, as the offset for m is not committed to Kafka
            equals@Assertions({
                actual = #queryResult.row
                expected = 1
                message = "Expected to find 1 messages but found '" + #queryResult.row[0] + "'"
            })()

            // We expect this message to be the one we just sent
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]
    }
}