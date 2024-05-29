from ...SafeProducer.serviceA import ServiceA
from ....KafkaTestTool.kafkaTestTool import KafkaTestTool
from ..assertions import Assertions

from ..testTypes import ProducerTestInterface

from runtime import Runtime
from reflection import Reflection
from time import Time
from console import Console
from json-utils import JsonUtils
from database import Database

interface ServiceAFailureInterface {
    RequestResponse:
        /// @BeforeAll
        setup_connections(void)(void),

        /// @BeforeAll
        init_default_testcase(void)(void),

        /// @Test
        No_updates_occur_if_Service_A_crashes_upon_message_receival(void)(void) throws AssertionError(string),

        /// @Test
        No_updates_occur_if_Service_A_crashes_after_updating_local_state(void)(void) throws AssertionError(string),

        /// @Test
        No_message_is_not_delivered_if_A_throws_committing_transaction(void)(void) throws AssertionError(string),
        
        /// @Test
        Message_is_delivered_if_A_fails_after_committing_transaction(void)(void) throws AssertionError(string),
        
        /// @Test
        Message_is_not_delivered_if_outboxService_fails_before_inserting_it_into_local_storage(void)(void) throws AssertionError(string),

        /// @Test
        Message_is_not_delivered_if_outboxService_crashes_before_update_is_committed(void)(void) throws AssertionError(string),

        /// @Test
        Message_is_delivered_if_MFS_fails_before_checking_for_new_messages(void)(void) throws AssertionError(string),

        /// @Test
        Message_is_delivered_if_MFS_fails_after_reading_message_from_outbox_table(void)(void) throws AssertionError(string),

        /// @Test
        Message_is_delivered_twice_if_MFS_fails_after_reading_message_from_outbox_table(void)(void) throws AssertionError(string),

        /// @AfterEach
        clear_tables(void)(void)
}

service ServiceAFailure{
    execution: sequential

    inputPort TestInterface {
        Location: "local"
        Interfaces: ServiceAFailureInterface 
    }
    
    embed Assertions as Assertions
    embed Database as Database
    embed Time as Time
    embed Runtime as Runtime
    embed Console as Console
    embed Reflection as Reflection
    embed JsonUtils as JsonUtils

    embed KafkaTestTool as KafkaTestTool
    embed ServiceA as ServiceA

    main
    {
        //  -------------- Setup and teardown tests:
        [setup_connections()(){
            // Connect to db
            println@Console("\n\n------------------ Connecting to Database ------------------")()
            with ( connectionInfo ){
                .username = "postgres"
                .password = "example"
                .database = "service-a-db"
                .driver = "postgresql"
                .host = ""
            }
            connect@Database(connectionInfo)()
            
            // Setup the Kafka test tool:
            println@Console("\n\n------------------ Starting Kafka Test Tool ------------------")()
            setupTestConsumer@KafkaTestTool({
                .bootstrapServer = "localhost:29092"        // As seen in .serviceA
                .topic = "a-out"                            // As seen in .serviceA
            })()

            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM Customers WHERE true;")()
            update@Database("DELETE FROM outbox WHERE true;")()
            update@Database("INSERT INTO Customers VALUES('user1', 0);")()
        }]

        [init_default_testcase()(){
            global.DefaultTestCase << {
                .serviceA << {
                    .throw_on_recieved = false
                    .throw_after_update_local = false
                    .throw_before_commit = false
                    .throw_after_commit = false
                }
                .outboxtests << {
                    .throw_before_insert = false
                    .throw_after_insert = false
                }
                .mfsTests << {
                    .throw_before_check_for_messages = false
                    .throw_after_message_found = false
                    .throw_after_send_but_before_delete = false
                }
            }
        }]

        [clear_tables()(){
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM Customers WHERE true;")()
            update@Database("DELETE FROM outbox WHERE true;")()
            update@Database("INSERT INTO Customers VALUES('user1', 0);")()
        }]



        //  -------------- Service A Tests:
        [No_updates_occur_if_Service_A_crashes_upon_message_receival()(){
            println@Console("Executing test No_updates_occur_if_Service_A_crashes_upon_message_receival")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.serviceA.throw_on_recieved = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)

            // Nothing should be written in the Customers table
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 0
                message = "Expected number for user1 to be '" + expected +  "' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "No records found"
                message = "Expected the message from kafka to be " + expected + ", but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        [No_updates_occur_if_Service_A_crashes_after_updating_local_state()(){
            println@Console("Executing test No_updates_occur_if_Service_A_crashes_after_updating_local_state")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.serviceA.throw_after_update_local = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)

            // Nothing should be written in the Customers table
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 0
                message = "Expected number for user1 to be '" + expected +  "' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "No records found"
                message = "Expected the message from kafka to be " + expected + ", but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        [No_message_is_not_delivered_if_A_throws_committing_transaction()(){
            println@Console("Executing test No_message_is__not_delivered_if_A_throws_committing_transaction")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.serviceA.throw_before_commit = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)

            // Nothing should be written in the Customers table
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 0
                message = "Expected number for user1 to be '" + expected +  "' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "No records found"
                message = "Expected the message from kafka to be " + expected + ", but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        [Message_is_delivered_if_A_fails_after_committing_transaction()(){
            println@Console("Executing test Message_is_delivered_if_A_fails_after_committing_transaction")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.serviceA.throw_after_commit = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
             scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)

            // The outbox should still insert the message in the table
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 1
                message = "Expected number for user1 to be '" + expected +  "' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "Recieved"
                message = "Expected the message from kafka to be " + expected + ", but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        [Message_is_not_delivered_if_outboxService_fails_before_inserting_it_into_local_storage()(){
            println@Console("Executing test Message_is_not_delivered_if_outboxService_fails_before_inserting_it_into_local_storage")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.outboxtests.throw_before_insert = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
             scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)

            // The outbox crashes, so the message is not inserted
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 0
                message = "Expected number for user1 to be '0' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "No records found"
                message = "Expected the message from kafka to be " + expected + ", but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        [Message_is_not_delivered_if_outboxService_crashes_before_update_is_committed()(){
            println@Console("Executing test Message_is_not_delivered_if_outboxService_crashes_before_update_is_committed")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.outboxtests.throw_after_insert = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
             scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)

            // The outbox crashes, so the message is not inserted
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 0
                message = "Expected number for user1 to be '0' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "No records found"
                message = "Expected the message from kafka to be " + expected + ", but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        // All MFS failures should result in the message still being sent, as it persists in the outbox
        [Message_is_delivered_if_MFS_fails_before_checking_for_new_messages()(){
            println@Console("Executing test Message_is_delivered_if_MFS_fails_before_checking_for_new_messages")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.mfsTests.throw_before_check_for_messages = false
            println@Console("\nHeyyyy: " + TestCase.mfsTests.throw_before_check_for_messages + "\n")()
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
             scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )

            readSingle@KafkaTestTool()(kafkaResponse)

            // MFS crashes and restarts, and should then deliver the message as needed
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 1
                message = "Expected number for user1 to be '1' but found '" +  databaseRows.row[0].number + "'"
            })()
            
            // The message should be delivered properly
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "Recieved"
                message = "Expected the message from kafka to be 'Recieved', but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        [Message_is_delivered_if_MFS_fails_after_reading_message_from_outbox_table()(){
            println@Console("Executing test Message_is_delivered_if_MFS_fails_after_reading_message_from_outbox_table")()

            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.mfsTests.throw_after_message_found = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
             scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            readSingle@KafkaTestTool()(kafkaResponse)
            query@Database("SELECT * FROM Customers")( databaseRows )

            // The outbox crashes, so the message is not inserted
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 1
                message = "Expected number for user1 to be '1' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "Recieved"
                message = "Expected the message from kafka to be 'Recieved', but recieved: '" + kafkaResponse.status + "'"
            })()
        }]

        [Message_is_delivered_twice_if_MFS_fails_after_reading_message_from_outbox_table()(){
            println@Console("Executing test Message_is_delivered_twice_if_MFS_fails_after_reading_message_from_outbox_table")()
            // Arrange
            TestCase << global.DefaultTestCase
            TestCase.mfsTests.throw_after_send_but_before_delete = true
            setupProducerTests@ServiceA(TestCase)()
            
            // Act
             scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                startChoreography@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM Customers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)
            readSingle@KafkaTestTool()(kafkaResponse2)

            // The outbox crashes, so the message is not inserted
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 1
                message = "Expected number for user1 to be '1' but found '" +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "Recieved"
                message = "Expected the message from kafka to be " + "Recieved" + ", but recieved: '" + kafkaResponse.status + "'"
            })()

            equals@Assertions({
                actual = kafkaResponse2.status
                expected = kafkaResponse.status
                message = "Expected to recieve 2 messages, but found: " + kafkaResponse.status + " and '" + kafkaResponse2.status + "'"
            })()
        }]
    }
}