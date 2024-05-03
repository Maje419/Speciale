from database import ConnectionInfo

type KafkaOptions: void {   
    .topic: string                              // The topic to write updates to
    .bootstrapServer: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
}

type PollSettings: void{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type UpdateOutboxRequest{
    .txHandle: long                                   // The transaction handle
    .operation: string                                  // The operation to be called on the receiver service
    .data: string                                       // A json representation of the request to pass to .operation
    .topic: string                                      // The kafka topic on which the update should be broadcast
}

type OutboxConfig{
    .databaseConnectionInfo: ConnectionInfo             // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                         // Object of type PollSettings descibing the desired behaviour of the MessageForwarder
    .brokerOptions: KafkaOptions //| RabbitMqOptions
    .databaseServiceLocation: any                       // The location of the Database service
}

type StatusResponse {
    .success: bool
    .message: string
}

//-------------------- MFS Types -----------------------//
type ColumnSettings {
    .keyColumn: string
    .valueColumn: string
    .idColumn: string
}

type MFSParams{
    .databaseConnectionInfo: ConnectionInfo
    .pollSettings: PollSettings
    .columnSettings: void{
        .keyColumn: string
        .valueColumn: string
        .idColumn: string
    }
    .brokerOptions: KafkaOptions
}

//-------------------- Kafka Inserter Types -----------------//
type KafkaMessage {
    .topic: string
    .key: string
    .value: string
    .brokerOptions: KafkaOptions
}