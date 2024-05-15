//############################ Types needed for a service to embed the IBOB service ############################\\
/** Options which configure which Kafka broker the producer should write to */
type KafkaOutboxOptions: void {   
    .topic: string                                  //< The topic to write updates to
    .bootstrapServer: string                        //< The URL of the kafka server to connect to, e.g. "localhost:9092"
}

/** Settings which configure the outbox service. */
type OutboxConfig{
    .pollTimer[0,1]: int                                //< How many ms the the outbox will wait between checking for new messages
    .pollAmount[0,1]: int                                    //< The amount of messages which are read from the outbox table each poll
    .brokerOptions: KafkaOutboxOptions                  //< Object of type KafkaOptions describing the intended configuration for the producer
    .databaseServiceLocation: any                       //< The location of the DatabaseService which the outbox will use
}

/** A request to insert some message into the outbox */
type UpdateOutboxRequest{
    .txHandle: long                                 //< The transaction handle
    .operation: string                              //< The operation to be called on the receiver service
    .parameters: string                             //< A json representation of the request to pass to .operation
    .topic: string                                  //< The kafka topic on which the update should be broadcast
}


/** A response object for messages being inserted into kafka */
type StatusResponse {
    .success: bool                                  //< Whether the call was a success
    .message: string                                //< A string describing the message which was inserted into kafka, or the exception message if one occured 
}

/** The interface that the user needs to send messages to in order for the IBOB service to forward them correctly */
interface OutboxInterface{
    RequestResponse:
        updateOutbox( UpdateOutboxRequest )( StatusResponse )
}