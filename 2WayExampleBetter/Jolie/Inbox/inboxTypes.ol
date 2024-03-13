
//#################### General types #######################\\
type KafkaOptions: void {   
    .bootstrapServer: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
    .topic: string
}

type RabbitMqOptions {      // Not implemented
    .bootstrapServer: string
    .groupId: string
}

type PollOptions: void{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type KafkaMessage {
    .offset: long
    .key: string
    .value: string
    .topic: string
}

//#################### InboxService types #######################
type InboxConfig: void {
    .localLocation: any
    .ibobLocation[0,1]: any
    .externalLocation[0,1]: string
    .databaseConnectionInfo : any {?}    // This should really be of type ConnectionInfo, but cannot make it work with imports
    .transactionServiceLocation: any
    .kafkaPollOptions: PollOptions
    .kafkaInboxOptions: KafkaOptions
}

type MRSEmbeddingConfig {
    .inboxServiceLocation: any
    .kafkaPollOptions: PollOptions
    .kafkaInboxOptions: KafkaOptions
}


// ------- InboxReader Types ---------


// ------- InboxWriter Types ---------

type InboxWriterInsertRequest{
    .operation: string                      // The operation to be called
    .request*: any {?}                      // The request which was to be sent to the operation, as a Jolie structure
    .id[0,1] : string | void
}

//#################### MessageRetrieverService types #######################
type ConsumeRequest{
    .timeoutMs: long
}

type ConsumerRecord {
    .status: int
    .messages*: KafkaMessage
}

type CommitRequest {
    .offset: long
}

type CommitResponse {
    .status: int
    .reason: string
}

type InitializeConsumerRequest {
    .pollOptions: PollOptions
    .brokerOptions: KafkaOptions   
}