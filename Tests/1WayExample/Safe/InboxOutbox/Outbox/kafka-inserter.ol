from ..publicOutboxTypes import StatusResponse, KafkaOutboxOptions

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
