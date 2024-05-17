/** Response object showing whether the consumer was sucessfully created */ 
type InitializeConsumerResponse: bool

/** A message going out of Kafka */
type KafkaMessage {
    .offset: long                               //< The offset Kafka has given the message
    .key: string                                //< The key of the message
    .value: string                              //< The value assosiated with the key
    .topic: string                              //< The topic this message belongs to
}

/** The response to a consume request, sent from the Java Kafka service to MRS */
type ConsumerRecord {
    .status: int                            //< 0 if the consume request was successful, 1 if something went wrong
    .messages*: KafkaMessage                //< A list of the new messages found in Kafka
}

/** Repressents the options to be defined to retrieve messages from kafka */
type KafkaInboxOptions: void {
    .pollAmount[0, 1]: int                            //< How many messages can be retrieved from Kafka time the consumer is polled
    .pollTimeout[0, 1]: int                           //< How many ms the consumer will wait before returning that no new messages were found
    .bootstrapServer: string                    //< The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            //< The group id that the consumer should be part of
    .topic: string                              //< The Kafka topic which the Inbox should listen on
}

interface SimpleKafkaConsumerInterface {
    RequestResponse: 
        initialize( KafkaInboxOptions ) ( InitializeConsumerResponse ),
        consume( void )( ConsumerRecord ),
}

service KafkaConsumer{
    inputPort Input {
        Location: "local"
        Interfaces: SimpleKafkaConsumerInterface
        } 
        foreign java {
            class: "jolie.kafka.consumer.UsafeKafkaConsumerService"
        }
}