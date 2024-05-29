from .internalInboxTypes import InitializeConsumerResponse, ConsumerRecord, CommitRequest, CommitResponse
from ..publicInboxTypes import KafkaInboxOptions

interface SimpleKafkaConsumerInterface {
    RequestResponse: 
        initialize( KafkaInboxOptions ) ( InitializeConsumerResponse ),
        consume( void )( ConsumerRecord ),
        commit( CommitRequest )( CommitResponse ) 
}

service KafkaConsumer{
    inputPort Input {
        Location: "local"
        Interfaces: SimpleKafkaConsumerInterface
        } 
        foreign java {
            class: "jolie.kafka.consumer.KafkaConsumerService"
        }
}