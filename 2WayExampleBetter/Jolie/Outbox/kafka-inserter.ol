from .outboxTypes import KafkaMessage, StatusResponse

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
