/** A response object for messages being inserted into kafka */
type StatusResponse {
    .success: bool                                  //< Whether the call was a success
    .message: string                                //< A string describing the message which was inserted into kafka, or the exception message if one occured 
}

/** Options which configure which Kafka broker the producer should write to */
type KafkaOutboxOptions: void {   
    .topic: string                                  //< The topic to write updates to
    .bootstrapServer: string                        //< The URL of the kafka server to connect to, e.g. "localhost:9092"
}

/** A type repressenting a message to insert into kafka */
type KafkaMessage {
    .topic: string                                  //< The topic the message should be written to
    .key: string                                    //< The key of the message
    .value: string                                  //< The value of the message
    .brokerOptions: KafkaOutboxOptions                    //< Information about which kafka broker the message should be written to 
}

interface KafkaInserterInterface {
    RequestResponse: 
        propagateMessage( KafkaMessage )( StatusResponse ),
}

/**
*   A connector class to allow for communicating with the java-service (defined in ~/JavaServices/kafka-producer/src/main/java/example/KafkaRelayer.java)
*/
service KafkaInserter{
    inputPort Input {
        location: "local"
        interfaces: KafkaInserterInterface
        }
        foreign java {
            class: "example.KafkaRelayer"
        }
}
