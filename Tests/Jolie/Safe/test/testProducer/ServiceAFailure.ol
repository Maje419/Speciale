from ...SafeProducer.serviceA import ServiceA
from ....KafkaTestTool.kafkaTestTool import KafkaTestTool
from ..assertions import Assertions

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

        /// @Test
        No_message_enters_outbox_whem_A_throws_before_calling_outbox(void)(void) throws AssertionError(string),

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
                .bootstrapServer = "localhost:29092"        // As seen in .serviceA
                .topic = "example"                          // As seen in .serviceA.Outbox.MessageForwarderService
            })()

            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM numbers WHERE true;")()
            update@Database("DELETE FROM outbox WHERE true;")()
            update@Database("INSERT INTO numbers VALUES('user1', 0);")()
        }]

        [clear_tables()(){
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            sleep@Time(5000)()
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM numbers WHERE true;")()
            update@Database("DELETE FROM outbox WHERE true;")()
            update@Database("INSERT INTO numbers VALUES('user1', 0);")()

        }]

        //  -------------- Service A Tests:
        [No_message_enters_outbox_whem_A_throws_before_calling_outbox()(){
            // Arrange
            with (TestCase){
                .serviceA << {
                    .throw_before_outbox_call = true
                    .throw_after_outbox_call = false
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
            setupTest@ServiceA(TestCase)()
            
            // Act
             scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                updateNumber@ServiceA({.username = "user1"})()
            }

            // Assert
            query@Database("SELECT * FROM numbers")( databaseRows )
            readSingle@KafkaTestTool()(kafkaResponse)

            // Nothing should be written in the Numbers table
            equals@Assertions({
                actual = databaseRows.row[0].number
                expected = 0
                message = "Expected the 'number = 0' and found 'number = " +  databaseRows.row[0].number + "'"
            })()

            // No messages should be written to Kafka
            equals@Assertions({
                actual = kafkaResponse.status
                expected = "No records found"
                message = "Expected the message from kafka to be 'No records found', but recieved: '" + kafkaResponse.status + "'"
            })()
        }]
    }
}