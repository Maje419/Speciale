from ..publicInboxTypes import KafkaInboxOptions

/** A message going out of Kafka */
type KafkaMessage {
    .offset: long                               //< The offset Kafka has given the message
    .key: string                                //< The key of the message
    .value: string                              //< The value assosiated with the key
    .topic: string                              //< The topic this message belongs to
}

/** Configuration needed to initialize the Message Retriever Service */
type MRSEmbeddingConfig{
    .pollTimer: int
    .kafkaInboxOptions: KafkaInboxOptions
    .inboxWriterLocation: any 
}

//################ Types used in the Kafka-retriever ################\\
/** A request to consume a message from Kafka. Sent from the MRS to the Java Kafka service */
type ConsumeRequest: long //< Defines how many ms the Java Kafka service should wait for information from Kafak before timing out

/** The response to a consume request, sent from the Java Kafka service to MRS */
type ConsumerRecord {
    .status: int                            //< 0 if the consume request was successful, 1 if something went wrong
    .messages*: KafkaMessage                //< A list of the new messages found in Kafka
}

/** A request from the MRS to the Java Kafka Consumer to commit a given offset */
type CommitRequest: long                           //< The offset to commit.

/** The response from Java Kafka Consumer to MRS, telling whether a commit was successful */
type CommitResponse {
    .status: int                            //< 0 if the offset was committed correctly. 1 if some error occured.
    .reason: string                         //< A message explaining if the operation succeeded, or what went wrong.
}

/** Response object showing whether the consumer was sucessfully created */ 
type InitializeConsumerResponse: bool