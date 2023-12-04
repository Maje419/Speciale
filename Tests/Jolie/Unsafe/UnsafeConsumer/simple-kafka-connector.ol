type ConsumerRecord {
    .code: int
    .messages*: string
}

interface  SimpleKafkaConsumerInterface {
    RequestResponse: Consume( string )( ConsumerRecord )
    OneWay: Initialize ( string )
}

service SimpleKafkaConsumer{
    inputPort Input {
        Location: "local"
        Interfaces: SimpleKafkaConsumerInterface
        } 
        foreign java {
            class: "jolie.kafka.consumer.KafkaConsumerService"
        }
}