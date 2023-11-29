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
    .tHandle: string                                    // The transaction handle
    .commitTransaction: bool                            // If true, the transaction will be commited after executing sqlQuery
    .operation: string                                  // The operation to be called on the receiver service
    .data: string                                       // A json representation of the request to pass to .operation
    .topic: string                                      // The kafka topic on which the update should be broadcast
}

type OutboxSettings{
    .databaseConnectionInfo: ConnectionInfo             // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                        // Object of type PollSettings descibing the desired behaviour of the MessageForwarder
    .brokerOptions: KafkaOptions // RabbitMqOptions
    .transactionServiceLocation: any                    // The location of the transaction service
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

type ForwarderServiceInfo {
    .databaseConnectionInfo: ConnectionInfo     // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                // The settings to use
    .columnSettings: ColumnSettings            // The names of the columns in the 'outbox' table
    .brokerOptions: KafkaOptions
}
//-------------------- Kafka Inserter Types -----------------//
type KafkaMessage {
    .topic: string
    .key: string
    .value: string
    .brokerOptions: KafkaOptions
}