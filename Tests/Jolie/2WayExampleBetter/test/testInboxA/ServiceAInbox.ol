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

interface ServiceAInboxInterface {
    RequestResponse:
        /// @BeforeAll
        setup_connections(void)(void),
        
        /// @BeforeAll
        init_default_testcase(void)(void),

        /// @Test
        MRS_throws_on_message_found_does_not_desync(void)(void),

        /// @AfterEach
        clear_tables(void)(void)
}

service ServiceAInbox{
    execution: sequential

    inputPort TestInterface {
        Location: "local"
        Interfaces: ServiceAInboxInterface 
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

            global.config << config

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
                .inboxService << {
                    .throw_before_insert = false
                    .throw_after_insert = false
                } 
                .inboxWriterTests << {
                    .recieve_called_throw_before_update_local = false
                }
                .MRS << {
                    .throw_after_message_found = false
                    .throw_after_notify_inbox_but_before_commit_to_kafka = false
                }
            }
        }]

        [MRS_throws_on_message_found_does_not_desync()(){
            // Arrange
            testCase << global.DefaultTestCase
            testCase.MRS.throw_after_message_found = true

            setupTest@ServiceA( testCase )( response )
            sleep@Time(100)()

            // Act
            insertMessageFromB
            sleep@Time(10000)()

            // Assert
            // We expect the user 'user1' to have been inserted into the empty numbers table
            query@Database("SELECT * FROM numbers WHERE username = 'user1'")( rows )
            equals@Assertions({
                expected = 1
                actual = #rows.row
                message = "Expected to find 1 user, but found" + #rows.row
            })()
        }]
    }

    define insertMessageFromB
    {   
        jsonValue << {
            .mid = "803:1709557365584"
            .parameters << {
                .username = "user1"
            }
        }

        getJsonString@JsonUtils(jsonValue)(jsonString)
        println@Console("Inserting jsonString: " + jsonString)()

        kafkaMessage.topic = global.config.kafkaInboxOptions.topic        // "b-out"
        kafkaMessage.key = "updateNumber"                           // key is the operation
        kafkaMessage.value = jsonString                             // See above

        send@KafkaTestTool(kafkaMessage)( testToolResponse )

        println@Console("KafkaTestTool SendResponse: " + testToolResponse.status )()
        
    }
}