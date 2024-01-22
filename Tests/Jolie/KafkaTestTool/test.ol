from .kafkaTestTool import KafkaTestTool
from console import Console

service Main{
    embed KafkaTestTool as KafkaTestTool
    embed Console as Console

    main
    {
        with( kafkaSetup2 ){
            .bootstrapServer = "localhost:9092"
            .topic = "example"
        }

        with( kafkaSetup1 ){
            .bootstrapServer = "localhost:9092"
        }

        setupTestProducer@KafkaTestTool(kafkaSetup1)( producerResponse )
        println@Console( "producerResponse: " + producerResponse.status )()
        with (kafkaMessage){
            .topic = "example"
            .key = "Hello"
            .value = "World" 
        }

        send@KafkaTestTool(kafkaMessage)( sendResponse )
        println@Console( "sendResponse: " + sendResponse.status )()

        setupTestConsumer@KafkaTestTool( kafkaSetup2 )( setupResponse )
        println@Console("consumerResponse: " + setupResponse.status)()

        readSingle@KafkaTestTool()( kafkaResponse )
        println@Console("readSingleResponse: " + kafkaResponse.status)()

    }
}