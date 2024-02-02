from ..test.testTypes import InboxTestParams

type KafkaOptions: void {   
    .bootstrapServer: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
    .topic: string
}

type RabbitMqOptions {      // Not implemented
    .bootstrapServers: string
    .groupId: string
}

type PollOptions: void{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type InboxEmbeddingConfig: void {
    localLocation: any
    externalLocation[0,1]: any
}

type InitializeConsumerRequest{
    .pollOptions: PollOptions
    .brokerOptions: KafkaOptions
}

type KafkaMessage {
    .offset: long
    .key: string
    .value: string
    .topic: string
}

interface InboxInterface {
    RequestResponse: 
        recieveKafka( KafkaMessage )( string ),
        setupTest( InboxTestParams )( bool )
}