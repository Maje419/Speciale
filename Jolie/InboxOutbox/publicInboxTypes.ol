//############################ Types allowing for defining the default behaiour of the Inbox  ############################\\
/** Repressents the options to be defined to retrieve messages from kafka */
type KafkaInboxOptions: void {
    .pollAmount[0, 1]: int                            //< How many messages can be retrieved from Kafka time the consumer is polled
    .pollTimeout[0, 1]: int                           //< How many ms the consumer will wait before returning that no new messages were found
    .bootstrapServer: string                    //< The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            //< The group id that the consumer should be part of
    .topic: string                              //< The Kafka topic which the Inbox should listen on
}

/** Represents all information needed to initialize the Inbox service */
type InboxConfig: void {
    .pollTimer[0, 1]: int                   //< How often the Inbox should check for new messages (ms)
    .locations: void{
        .localLocation: any                 //< The location of the service wanting to embed the Inbox
        .databaseServiceLocation: any       //< The location of the Database Service
    }
    .kafkaOptions: KafkaInboxOptions        //< Options describing which Kafka instance should be contacted
}

//############################ Types needed for writing the InboxService  ############################\\

/** A request to enter some operation call into the outbox */
type IbobLocation : any                     //< The location of the Ibob service

type InboxWriterInsertRequest{
    .operation: string                      //< The operation to be called
    .request: any {?}                       //< The request which was to be sent to the operation, as a Jolie structure
    .id[0,1] : string | void                //< Some message ID
}

/** This interface can be called by the user to insert a new message in the inbox table */
interface InboxWriterExternalInterface {
    RequestResponse:
        /** This operation can be called by some embedder service wanting to insert a message into the inbox */
        insertIntoInbox( InboxWriterInsertRequest )( string )
}

