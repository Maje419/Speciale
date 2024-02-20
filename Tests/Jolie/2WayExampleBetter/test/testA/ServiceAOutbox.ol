from ...Jolie.ServiceA.serviceA import ServiceA
from ....KafkaTestTool.kafkaTestTool import KafkaTestTool
from ..assertions import Assertions

from runtime import Runtime
from reflection import Reflection
from time import Time
from console import Console
from json-utils import JsonUtils
from database import Database
from file import File
from string_utils import StringUtils

interface ServiceAOutboxInterface {
    RequestResponse:
        /// @BeforeAll
        setup_connections(void)(void),
        
        /// @BeforeAll
        init_default_testcase(void)(void),

        /// @Test
        No_errors_are_thrown_causes_message_into_kafka(void)(void) throws AssertionException,
        
        /// @Test
        ServiceA_throws_on_updateNumber_causes_no_kafka_message(void)(void) throws AssertionException, 
        
        /// @Test
        ServiceA_throws_after_local_transaction_updates_causes_no_kafka_message(void)(void) throws AssertionException,
        
        /// @Test
        OutboxService_throws_upon_called_causes_no_local_update_and_no_kafka_message(void)(void) throws AssertionException,
        
        /// @Test
        OutboxService_throws_after_transaction_committed_results_in_local_update_and_kafka_message(void)(void) throws AssertionException,
        
        /// @Test
        MFS_throw_on_message_found_tries_again_to_forward_same_message(void)(void) throws AssertionException,
        
        /// @Test
        MFS_throw_before_commit_to_kafka_results_in_message_sent_twice(void)(void) throws AssertionException,

        /// @AfterEach
        clear_tables(void)(void)
}

service ServiceAOutbox{
    execution: sequential

    inputPort TestInterface {
        Location: "local"
        Interfaces: ServiceAOutboxInterface 
    }
    
    embed Assertions as Assertions
    embed Database as Database
    embed Time as Time
    embed Runtime as Runtime
    embed Console as Console
    embed Reflection as Reflection
    embed JsonUtils as JsonUtils
    embed File as File
    embed StringUtils as StringUtils

    embed KafkaTestTool as KafkaTestTool
    embed ServiceA as ServiceA

    main
    {
        //  -------------- Setup and teardown tests:
        [setup_connections()(){
            // Connect to db
            println@Console("\n\n------------------ Connecting to Database ------------------")()

            readFile@File(
            {
                filename = "Jolie/ServiceA/serviceAConfig.json"
                format = "json"
            }) ( config )

            connect@Database(config.serviceAConnectionInfo)()

            println@Console("\n\n------------------ Starting Kafka Test Tool ------------------")()

            // Initialize the consumer to read from the same topic that ServiceA writes to
            setupTestConsumer@KafkaTestTool({
                bootstrapServer = config.kafkaInboxOptions.bootstrapServer        // As seen in .serviceAConfig
                topic = config.kafkaOutboxOptions.topic                             // As seen in .serviceAConfig
            })()
            
            setupTestProducer@KafkaTestTool({
                bootstrapServer =  config.kafkaOutboxOptions.bootstrapServer        // As seen in .serviceAConfig
            })()

            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM numbers WHERE true;")()
            update@Database("DELETE FROM inbox WHERE true;")()
            update@Database("DELETE FROM outbox WHERE true;")()
        }]

        [clear_tables()(){
            // Sleep here to avoid having multiple tests manipulating the same dabatase file
            sleep@Time(1000)()
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM numbers WHERE true;")()
            update@Database("DELETE FROM inbox WHERE true;")()
            update@Database("DELETE FROM outbox WHERE true;")()
        }]

        [init_default_testcase()(){
            global.DefaultTestCase << {
                .serviceA << {
                    .throw_on_updateNumber_called = false
                    .throw_after_local_updates_executed = false
                    .throw_on_finalizeChoreography_called = false
                    .throw_after_transaction_committed = false
                }
                .outboxService << {
                    .throw_on_updateOutbox_called = false
                    .throw_after_transaction_committed = false
                }
                .MFS << {
                    .throw_after_message_found = false
                    .throw_before_commit_to_kafka = false
                }
            }
        }]

        [No_errors_are_thrown_causes_message_into_kafka()(){
            // Arrange
            setupTest@ServiceA(global.DefaultTestCase)()

            // Act
            updateNumber@ServiceA({username = "user1"})(resp)


            //Assert
            readSingle@KafkaTestTool()(response)
            readSingle@KafkaTestTool()(response2)

            equals@Assertions({
                expected = "Recieved"
                actual = response.status
                message = "Expected to find a message in Kafka, but found: " + response.status
            })()

            equals@Assertions({
                expected = "No records found"
                actual = response2.status
                message = "Expected to not find a message in Kafka, but found: " + response2.status
            })()
        }]

        [ServiceA_throws_on_updateNumber_causes_no_kafka_message()(){
            // Arrange
            testcase << global.DefaultTestCase
            testcase.serviceA.throw_on_updateNumber_called = true

            setupTest@ServiceA(testcase)()

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                updateNumber@ServiceA({.username = "user1"})()
            }

            //Assert
            readSingle@KafkaTestTool()(response)
            equals@Assertions({
                expected = "No records found"
                actual = response.status
                message = "Expected to not find a message in Kafka, but found: " + response.status
            })()
        }]

        [ServiceA_throws_after_local_transaction_updates_causes_no_kafka_message()(){
            // Arrange
            testcase << global.DefaultTestCase
            testcase.serviceA.throw_after_local_updates_executed = true

            setupTest@ServiceA(testcase)()

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                updateNumber@ServiceA({.username = "user1"})()
            }

            //Assert
            readSingle@KafkaTestTool()(response)
            equals@Assertions({
                expected = "No records found"
                actual = response.status
                message = "Expected to not find a message in Kafka, but found: " + response.status
            })()
        }]

        // TODO: Write tests for finalizing choreography, E.G reading from Kafka

        [OutboxService_throws_upon_called_causes_no_local_update_and_no_kafka_message()(){
            
            // Arrange
            testcase << global.DefaultTestCase
            testcase.outboxService.throw_on_updateOutbox_called = true

            setupTest@ServiceA(testcase)()

            // Act
            install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
            updateNumber@ServiceA({.username = "user1"})()

            //Assert
            readSingle@KafkaTestTool()(response)

            equals@Assertions({
                expected = "No records found"
                actual = response.status
                message = "Expected to not find a message in Kafka, but found: " + response.status
            })()

            query@Database("SELECT * FROM numbers WHERE username = 'user1'")( rows )
            equals@Assertions({
                expected = 0
                actual = #rows.row
                message = "Expected to find 0 users, but found" + #rows.row
            })()
        }]

        [OutboxService_throws_after_transaction_committed_results_in_local_update_and_kafka_message()(){
            // Arrange
            testcase << global.DefaultTestCase
            testcase.outboxService.throw_after_transaction_committed = true

            setupTest@ServiceA(testcase)()

            // Act
            install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
            updateNumber@ServiceA({.username = "user1"})()

            //Assert
            readSingle@KafkaTestTool()(response)

            equals@Assertions({
                expected = "Recieved"
                actual = response.status
                message = "Expected to find a message in Kafka, but found: " + response.status
            })()

            // We expect the user 'user1' to have been inserted into the empty numbers table
            query@Database("SELECT * FROM numbers WHERE username = 'user1'")( rows )
            equals@Assertions({
                expected = 1
                actual = #rows.row
                message = "Expected to find 1 user, but found" + #rows.row
            })()
        }]

        [MFS_throw_on_message_found_tries_again_to_forward_same_message()(){
            // Arrange
            testcase << global.DefaultTestCase
            testcase.MFS.throw_after_message_found = true

            setupTest@ServiceA(testcase)()

            // Act
            install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
            updateNumber@ServiceA({.username = "user1"})()

            //Assert
            readSingle@KafkaTestTool()(response)

            equals@Assertions({
                expected = "Recieved"
                actual = response.status
                message = "Expected to find a message in Kafka, but found: " + response.status
            })()

            // We expect the user 'user1' to have been inserted into the empty numbers table
            query@Database("SELECT * FROM numbers WHERE username = 'user1'")( rows )
            equals@Assertions({
                expected = 1
                actual = #rows.row
                message = "Expected to find 1 user, but found" + #rows.row
            })()
        }]
    
        [MFS_throw_before_commit_to_kafka_results_in_message_sent_twice()(){
                // Arrange
                testcase << global.DefaultTestCase
                testcase.MFS.throw_before_commit_to_kafka = true

                setupTest@ServiceA(testcase)()

                // Act
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                updateNumber@ServiceA({.username = "user1"})()

                //Assert
                readSingle@KafkaTestTool()(response)
                readSingle@KafkaTestTool()(response2)

                equals@Assertions({
                    expected = "Recieved"
                    actual = response.status
                    message = "Expected to find a message in Kafka, but found: " + response.status
                })()

                equals@Assertions({
                    expected = "Recieved"
                    actual = response2.status
                    message = "Expected to find a message in Kafka, but found: " + response.status
                })()
                
                response.message.regex = "Value = "
                response2.message.regex = "Value = "
                split@StringUtils( response.message )( firstMessageValue )
                split@StringUtils( response2.message )( secondMessageValue )

                // Since the same message was sent twice, we expect the value of the messages to be the same (including the unique mid)
                equals@Assertions({
                    expected = firstMessageValue.result[1]
                    actual = secondMessageValue.result[1]
                    message = "Expected to find: '" + firstMessageValue.result[1] + "'' but found: '" + secondMessageValue.result[1] + "'"
                })()
            }]
    }
}