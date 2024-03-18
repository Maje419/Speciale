from database import ConnectionInfo

/** Options which configure which Kafka broker the producer should write to */
type KafkaOptions: void {   
    .topic: string                                  //< The topic to write updates to
    .bootstrapServer: string                        //< The URL of the kafka server to connect to, e.g. "localhost:9092"
}

/** Settings which configure the behaviour of the MFS */
type PollSettings: void{
    .pollDurationMS: int                            //< How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                                //< The amount of messages which are read from the 'messages' table per duration
}

/** A request to insert some message into the outbox */
type UpdateOutboxRequest{
    .tHandle: string                                //< The transaction handle
    .operation: string                              //< The operation to be called on the receiver service
    .data: string                                   //< A json representation of the request to pass to .operation
    .topic: string                                  //< The kafka topic on which the update should be broadcast
}

/** Settings which configure the outbox service. */
type OutboxConfig{
    .databaseConnectionInfo: ConnectionInfo             //< The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                         //< Object of type PollSettings descibing the desired behaviour of the MessageForwarder
    .brokerOptions: KafkaOptions                        //< Object of type KafkaOptions describing the intended configuration for the producer
    .transactionServiceLocation: any                    //< The location of the transaction service
}

/** A response object for messages being inserted into kafka */
type StatusResponse {
    .success: bool                                  //< Whether the call was a success
    .message: string                                //< A string describing the message which was inserted into kafka, or the exception message if one occured 
}

//-------------------- MFS Types -----------------------//
/** Information about the columns of the outbox table */
type ColumnSettings {
    .keyColumn: string                              //< The name of the key column
    .valueColumn: string                            //< The name of the value column
    .idColumn: string                               //< The name of the id column
}

/** Parameters allowing for configuring the functionality of the MFS */
type MFSParams{
    .databaseConnectionInfo: ConnectionInfo         //< Which database should be used for the outbox table
    .pollSettings: PollSettings                     //< Which settings the MFS should use when polling the outbox table
    .columnSettings: ColumnSettings                 //< Information about the columns of the outbox table
    .brokerOptions: KafkaOptions                    //< Information about which kafka broker should be written to
}

//-------------------- Kafka Inserter Types -----------------//
/** A type repressenting a message to insert into kafka */
type KafkaMessage {
    .topic: string                                  //< The topic the message should be written to
    .key: string                                    //< The key of the message
    .value: string                                  //< The value of the message
    .brokerOptions: KafkaOptions                    //< Information about which kafka broker the message should be written to 
}