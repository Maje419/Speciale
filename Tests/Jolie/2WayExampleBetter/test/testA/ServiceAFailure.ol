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

interface ServiceAFailureInterface {
    RequestResponse:
        /// @BeforeAll
        setup_connections(void)(void),

        /// @Test
        test(void)(void) throws AssertionException,

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
    embed File as File

    embed KafkaTestTool as KafkaTestTool

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
            
            // Setup the Kafka test tool:
            println@Console("\n\n------------------ Starting Kafka Test Tool ------------------")()
            setupTestConsumer@KafkaTestTool({
                bootstrapServer = config.kafkaInboxOptions.bootstrapServer        // As seen in .serviceAConfig
                topic = config.kafkaInboxOptions.topic                             // As seen in .serviceAConfig
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

        [test()(){
            // Arrange

            // Act

            //Assert
        }]
    }
}