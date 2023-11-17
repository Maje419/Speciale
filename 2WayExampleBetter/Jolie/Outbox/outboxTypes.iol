//--------------------- Outbox Service Types --------------------//

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
    .tHandle: string                                     // The transaction handle
    .commitTransaction: bool                            // If true, the transaction will be commited after executing sqlQuery
    .key: string                                        // The key to use in the kafka message
    .value: string                                      // The value for the kafka message
    .topic: string                                      // The kafka topic on which the update should be broadcast
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

//-------------------- Kafka Inserter Types -----------------//