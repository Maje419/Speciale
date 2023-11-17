include "Outbox/outboxTypes.iol"

type KafkaMessage {
    .topic: string
    .key: string
    .value: string
    .brokerOptions: KafkaOptions
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
