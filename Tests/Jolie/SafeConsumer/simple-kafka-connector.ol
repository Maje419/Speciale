include "inboxTypes.iol"

type ConsumeRequest{
    .timeoutMs: long
}

type ConsumerRecord {
    .status: int
    .messages*: KafkaMessage
}

type KafkaMessage {
    .offset: long
    .key: string
    .value: string
    .topic: string
}

type CommitRequest {
    .offset: long
}

type CommitResponse {
    .response: string
}

interface SimpleKafkaConsumerInterface {
    RequestResponse: 
        Initialize( InitializeConsumerRequest ) ( KafkaOptions ),
        Consume( ConsumeRequest )( ConsumerRecord ),
        Commit( CommitRequest )( CommitResponse ) 
}

service SimpleKafkaConsumerConnector{
    inputPort Input {
        Location: "local"
        Interfaces: SimpleKafkaConsumerInterface
        } 
        foreign java {
            class: "jolie.kafka.consumer.KafkaConsumerService"
        }
}