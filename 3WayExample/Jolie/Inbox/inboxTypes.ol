
//#################### General types #######################\\
/** Represents the options that can be passed to Kafka Consumers or Producers used for connecting with Kafka */
type KafkaOptions: void {   
    .bootstrapServer: string                    //< The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            //< The group id of the kafka cluster to send messages to
    .topic: string                              //< The topic that the message is attatched to
}

/** Represents options that configure the way checking for new messages is done*/
type PollOptions: void{
    .pollDurationMS: int                        //< tODO: Unused, remove
    .pollAmount: int                            //< The number of messages the Java Kafka Consumer can return each time 'consume' is called
}

/** A message going out of Kafka */
type KafkaMessage {
    .offset: long                               //< The offset Kafka has given the message
    .key: string                                //< The key of the message
    .value: string                              //< The value assosiated with the key
    .topic: string                              //< The topic this message belongs to
}

//#################### InboxService types #######################
/** Represents all information needed to initialize the Inbox service */
type InboxConfig: void {
    .localLocation: any                 //< The location of the service wanting to embed the Inbox
    .ibobLocation[0,1]: any             //< The location of the InboxOutbox service
    .externalLocation[0,1]: string      //< Doesn't work yet, still TODO
    .databaseConnectionInfo : any {?}   //< An object of type ConnectionInfo which is used to connect to a database
    .databaseServiceLocation: any       //< The location of the DatabaseService for the inbox to use
    .kafkaPollOptions: PollOptions      //< Which options should be used when reading messages from Kafka
    .kafkaInboxOptions: KafkaOptions    //< Configuration needed for initializing the KafkaConsumer
}

/** Represssents all information needed to initialize the MRS service*/
type MRSEmbeddingConfig {
    .inboxServiceLocation: any          //< The location of the InboxWriterService
    .kafkaPollOptions: PollOptions      //< Which options should be used when reading messages from Kafka
    .kafkaInboxOptions: KafkaOptions    //< Configuration needed for initializing the KafkaConsumer
}

// ------- InboxWriter Types ---------
/** A request to enter some operation call into the outbox */
type InboxWriterInsertRequest{
    .operation: string                      //< The operation to be called
    .request*: any {?}                      //< The request which was to be sent to the operation, as a Jolie structure
    .id[0,1] : string | void                //< Some message ID
}

//#################### MessageRetrieverService types #######################

/** A request to consume a message from Kafka. Sent from the MRS to the Java Kafka service */
type ConsumeRequest{
    .timeoutMs: long                        //< Defines how many ms the Java Kafka service should wait for information from Kafak before timing out
}

/** The response to a consume request, sent from the Java Kafka service to MRS */
type ConsumerRecord {
    .status: int                            //< 0 if the consume request was successful, 1 if something went wrong
    .messages*: KafkaMessage                //< A list of the new messages found in Kafka
}

/** A request from the MRS to the Java Kafka Consumer to commit a given offset */
type CommitRequest {
    .offset: long                           //< The offset to commit.
}

/** The response from Java Kafka Consumer to MRS, telling whether a commit was successful */
type CommitResponse {
    .status: int                            //< 0 if the offset was committed correctly. 1 if some error occured.
    .reason: string                         //< A message explaining if the operation succeeded, or what went wrong.
}

/** This type contains the options needed to initialize the Java Kafka Consumer service. */
type InitializeConsumerRequest {
    .pollOptions: PollOptions               //< Which options the Java Kafka Consumer should use when asked to poll Kafka
    .brokerOptions: KafkaOptions            //< Which broker the Java Kafka Consumer should connect to
}