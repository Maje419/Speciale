type SetupTestConsumerRequest{
    .bootstrapServer: string
    .topic: string
}

type SetupTestProducerRequest {
    .bootstrapServer: string
}

type SetupResponse {
    .status: string
}

type SendResponse {
    .status: string
}

type ReadSingleResponse {
    .status: string
    .message: string
}

type SendRequest {
    .topic: string
    .key: string
    .value: string
}

interface KafkaTestToolInterface {
    RequestResponse: 
        setupTestConsumer( SetupTestConsumerRequest )( SetupResponse ),
        setupTestProducer( SetupTestProducerRequest )( SetupResponse ),
        readSingle( void )( ReadSingleResponse ),
        send( SendRequest )( SendResponse )

}

service KafkaTestTool{
    inputPort Input {
        Location: "local"
        Interfaces: KafkaTestToolInterface
        } 
        foreign java {
            class: "com.mycompany.app.KafkaTestTool"
        }
}