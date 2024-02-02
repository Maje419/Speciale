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

        /// @Test
        test1(void)(void) throws AssertionError(string),
        
        // /// @Test
        // Services_remain_in_sync_if_B_throws_after_updating_local(void)(void) throws AssertionError(string),

        // /// @Test
        // Services_are_out_of_sync_if_MRS_throws_before_forwarding_message_to_B(void)(void) throws AssertionError(string),

        // /// @Test
        // Services_remain_in_sync_when_MRS_throws_after_messaging_B_and_a_single_message_was_sent(void)(void) throws AssertionError(string),

        // /// @Test
        // Services_are_out_of_sync_if_MRS_throws_before_recursing(void)(void) throws AssertionError(string),

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

        //  -------------- Service B Tests:
        
        [test1()(){
            // Arrange
            with (testCase){
                .serviceB << {
                    .throw_before_search_inbox = true
                    .throw_at_inbox_message_found = false
                    .throw_after_transaction = false
                }
                .inboxTests << {
                    .throw_on_message_found = false
                    .throw_before_updating_inbox = false
                    .throw_before_updating_main_service = false
                }
                .mrsTests << {
                    .throw_after_message_found = false
                    .throw_after_before_inbox_responds = false
                }
            }

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
            println@Console("Found row: " + queryResult.row[0].message)()
            println@Console("Found rows " + #queryResult.row)()
            equals@Assertions({
                actual = queryResult.row[0].message
                expected = "Test:user1"
                message = "Expected Test:user1 but found '" + queryResult.row[0].message + "'"
            })()
        }]
    }
}