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
        Single_message_is_not_delivered_if_B_throws_before_updating_local(void)(void) throws AssertionError(string),
        
        /// @Test
        B_is_eventually_consistent_when_it_fails_on_one_message(void)(void) throws AssertionError(string),

        /// @Test
        B_only_handles_one_if_two_messages_with_same_mid_is_recieved(void)(void) throws AssertionError(string),
        
        /// @Test
        Message_is_delivered_if_Inbox_service_throws_once_before_updating_inbox(void)(void) throws AssertionError(string),
        
        /// @Test
        Single_message_is_not_delivered_if_Inbox_Service_throws_before_calling_B(void)(void) throws AssertionError(string),
        
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
            with ( connectionInfo ) 
            {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
            };
            connect@Database(connectionInfo)()
            
            // Setup the Kafka test tool:
            println@Console("\n\n------------------ Starting Kafka Test Tool ------------------")()
            setupTestConsumer@KafkaTestTool({
                .bootstrapServer = "localhost:29092"      // As seen in .MRS for serviceB
                .topic = "example"                          // ^^ Same
            })()

            setupTestProducer@KafkaTestTool({
                .bootstrapServer = "localhost:29092"
            })()

            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM example WHERE true;")()
            update@Database("DELETE FROM inbox WHERE true;")()
        }]

        [clear_tables()(){
            // Sleep here to avoid having multiple tests manipulating the same dabatase file
            sleep@Time(1000)()
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM example WHERE true;")()
            update@Database("DELETE FROM inbox WHERE true;")()
        }]

        [init_default_testcase()(){
            global.DefaultTestCase << {
                .serviceB << {
                    .throw_before_search_inbox = false
                    .throw_at_inbox_message_found = false
                }
                .inboxTests << {
                    .throw_before_updating_inbox = false
                    .throw_before_updating_main_service = false
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

            setupTest@ServiceB(testCase)()
            with (message){
                .topic = "example"
                .key = "Test"
            }

            getJsonString@JsonUtils({
                mid = 0
                parameters = "user1"
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(2000)()

            // Assert
            query@Database( "Select * from example;" )( queryResult )
            
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
        
        [Single_message_is_not_delivered_if_B_throws_before_updating_local()(){
            /* 
            This test shows that in this iteration, there is no system in place to re-check the inbox if something fails while
            handling a message
            */
            
            // Arrange
            testCase << global.DefaultTestCase
            testCase.serviceB.throw_before_search_inbox = true

            setupTest@ServiceB(testCase)()
            with (message){
                .topic = "example"
                .key = "Test"
            }

            getJsonString@JsonUtils({
                mid = 124
                parameters = "user1"
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(2000)()

            // Assert
            query@Database( "Select * from example;" )( queryResult )

            // We expect both messages to be present in local storage at B
            equals@Assertions({
                actual = #queryResult.row
                expected = 0
                message = "Expected to find 0 messages but found '" + #queryResult.row + "'"
            })()
        }]

        [B_is_eventually_consistent_when_it_fails_on_one_message()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.serviceB.throw_at_inbox_message_found = true

            setupTest@ServiceB(testCase)()
            with (message1){
                .topic = "example"
                .key = "Test"
            }

            with (message2){
                .topic = "example"
                .key = "Test2"
            }

            getJsonString@JsonUtils({
                mid = 1
                parameters = "user1"
            })(message1.value)

            getJsonString@JsonUtils({
                mid = 2
                parameters = "user2"
            })(message2.value)

            // Act
                //Throws on this message
            send@KafkaTestTool(message1)()
                // Does not throw on this message
            send@KafkaTestTool(message2)()
            sleep@Time(2000)()


            // Assert
            query@Database( "Select * from example;" )( queryResult )

            // We expect both messages to be present in local storage at B
            equals@Assertions({
                actual = #queryResult.row
                expected = 2
                message = "Expected to find 2 messages but found '" + #queryResult.row + "'"
            })()

            // We even expect the messasges to have arrived in causal ordering
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]

        [B_only_handles_one_if_two_messages_with_same_mid_is_recieved()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.serviceB.throw_at_inbox_message_found = true

            setupTest@ServiceB(testCase)()
            with (message1){
                .topic = "example"
                .key = "Test"
            }

            with (message2){
                .topic = "example"
                .key = "Test2"
            }

            getJsonString@JsonUtils({
                mid = 1
                parameters = "user1"
            })(message1.value)

            getJsonString@JsonUtils({
                mid = 1
                parameters = "user1"
            })(message2.value)

            // Act
                //Throws on this message
            send@KafkaTestTool(message1)()
                // Does not throw on this message
            send@KafkaTestTool(message2)()
            sleep@Time(2000)()


            // Assert
            query@Database( "Select * from example;" )( queryResult )

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

        // -------------- InboxService Tests:

        [Message_is_delivered_if_Inbox_service_throws_once_before_updating_inbox()(){
            /* 
            In this case, MRS will not commit the offset for the message, and it will retry calling the InboxService with the same message 
            */

            // Arrange
            testCase << global.DefaultTestCase
            testCase.inboxTests.throw_before_updating_inbox = true

            setupTest@ServiceB(testCase)()
            with (message){
                .topic = "example"
                .key = "Test"
            }

            getJsonString@JsonUtils({
                mid = 0
                parameters = "user1"
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(2000)()


            // Assert
            query@Database( "Select * from example;" )( queryResult )
            
            // We expect there to be 1 message in the state of service B
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

        [Single_message_is_not_delivered_if_Inbox_Service_throws_before_calling_B()(){
            /*
            This case shows an inherent weakness, which is that InboxService needs to tell ServiceB that the inbox has been updated
            Therefore, if the inbox service crashes before notifying B, B will stay inconsistent until it is notified for some other message
            */

            // Arrange
            testCase << global.DefaultTestCase
            testCase.inboxTests.throw_before_updating_main_service = true

            setupTest@ServiceB(testCase)()
            with (message){
                .topic = "example"
                .key = "Test"
            }

            getJsonString@JsonUtils({
                mid = 0
                parameters = "user1"
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(2000)()

            // Assert
            query@Database( "Select * from example;" )( queryResult )
            
            equals@Assertions({
                actual = #queryResult.row
                expected = 0
                message = "Expected to find 0 messages but found '" + #queryResult.row[0] + "'"
            })()
        }]

        // -------------- MRS Tests:
        [Message_is_delivered_if_MRS_throws_immediatly_after_finding_a_message()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.mrsTests.throw_after_message_found = true

            setupTest@ServiceB(testCase)()
            with (message){
                .topic = "example"
                .key = "Test"
            }

            getJsonString@JsonUtils({
                mid = 0
                parameters = "user1"
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(3000)()

            // Assert
            query@Database( "Select * from example;" )( queryResult )
            
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
            time it polls kafka. When this happens, it will again call receiveKafka at the inbox service, but since the message is 
            unique in the inbox table, and the mid has not changed, the inbox will simply tell the MRS that 
            'hey, i've already handled this message. Please commit it again'
            */
            // Arrange
            testCase << global.DefaultTestCase
            testCase.mrsTests.throw_after_message_found = true

            setupTest@ServiceB(testCase)()
            with (message){
                .topic = "example"
                .key = "Test"
            }

            getJsonString@JsonUtils({
                mid = 0
                parameters = "user1"
            })(message.value)

            // Act
            send@KafkaTestTool(message)()
            sleep@Time(3000)()

            // Assert
            query@Database( "Select * from example;" )( queryResult )
            
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