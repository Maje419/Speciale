include "console.iol"
include "time.iol"
include "database.iol"
include "file.iol"
include "Inbox/inboxTypes.iol"
include "serviceAInterface.iol"

from runtime import Runtime
from ..TransactionService.transactionService import TransactionServiceInterface

service Inbox (p: InboxEmbeddingConfig){
    execution: concurrent

    // Used for embedding services to talk with the inbox
    inputPort InboxInput {
        Location: "local"
        Interfaces: 
            InboxInterface
    }

    // This service takes over handling of the external endpoint from the embedder
    inputPort ExternalInput {
        Location: "socket://localhost:8080"    //It would be nice if this could be sent with as a parameter
        Protocol: http{
            format = "json"
        }
        Interfaces: 
            ServiceAInterface
    }

    // Used when forwarding messages back to embedder
    outputPort EmbedderInput {
        location: "local"   // Overwritten in init
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    outputPort TransactionService {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: TransactionServiceInterface
    }
    embed Runtime as Runtime

    init
    {
        EmbedderInput.location = p.localLocation

        // The inbox itself embeds MRS, which polls Kafka for updates
        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "messageRetrieverService.ol"
            params << { 
                inboxServiceLocation << localLocation
                kafkaPollOptions << p.kafkaPollOptions
                kafkaInboxOptions << p.kafkaInboxOptions

            }
        })( MessageRetriever.location )
        
        // Make sure that the transaction service we're talking to is the same one as Service A.
        TransactionService.location = p.transactionServiceLocation
        scope ( createtable ) 
        {
            connect@Database( p.databaseConnectionInfo )()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (request VARCHAR (150), hasBeenRead BOOLEAN, kafkaOffset INTEGER, rowid SERIAL PRIMARY KEY, UNIQUE(kafkaOffset));" )( ret )
        }
        println@Console( "InboxServiceA Initialized" )(  )
    }
    main{
        
        [updateNumber( req )( res ){
            // This method takes messages which come from outside the Jolie runtime, and stores them in the inbox
            // It is assumed that every message is unique, otherwise the protocol must dictate some id for incomming messages

            scope( MakeIdempotent ){
                // This should never throw, since the offset is set to NULL. We assume all external messages are unique for now
                install( SQLException => println@Console("Message already recieved, commit request")() )
                // Insert the request into the inbox table in the form:
                    // ___________________________________________________
                    // |            request        | hasBeenRead | offset |
                    // |———————————————————————————|—————————————|————————|
                    // | 'operation':'parameter(s)'|   'false'   |  NULL  |
                    // |——————————————————————————————————————————————————|
                // Insert the update into the 'inbox' table
                update@Database("INSERT INTO inbox (request, hasBeenRead, kafkaOffset) VALUES ('udateNumber:" + req.username + "', false, NULL)")()
            }
            res << "Message stored"
        }] 
        {
            // Initialize a new transaction to pass onto Service A
            initializeTransaction@TransactionService()(tHandle)

            // Within that transaction, update the inbox table for the received message to indicate that it has been read
            with (updateRequest){
                .update = "UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = NULL AND hasBeenRead = false"
                .handle = tHandle
            }

            executeUpdate@TransactionService( updateRequest )( updateResponse )
            
            req.handle = tHandle

            // Call the corresponding operation at Service A
            updateNumber@EmbedderInput( req )( embedderResponse )
        }

        [recieveKafka( req )( res ) {
            // Kafka messages for our inbox/outbox contains the operation invoked in the 'key', and the parameters in the 'value'
            connect@Database(config.serviceAConnectionInfo)()
            scope( MakeIdempotent ){
                // If this exception is thrown, Kafka some commit message must have disappeared. Resend it.
                install( SQLException => {
                    println@Console("Message already recieved, commit request")();
                    res = "Message already recieveid, please re-commit"
                })
                // Insert the request into the inbox table in the form:
                    // ___________________________________________________
                    // |            request        | hasBeenRead | offset |
                    // |———————————————————————————|—————————————|————————|
                    // | 'operation':'parameter(s)'|   'false'   | offset |
                    // |——————————————————————————————————————————————————|
                update@Database("INSERT INTO inbox (request, hasBeenRead, kafkaOffset) VALUES (
                    '"+ req.key + ":" + req.value +        // numbersUpdated:user1
                    "', false, " +                         // false
                    req.offset + ")")()                           // offset
            }
            res << "Message stored"
        }] 
        {   
            // Initialize a new transaction to pass onto Service A
            initializeTransaction@TransactionService()(tHandle)

            with (updateRequest){
                .handle = tHandle
                .update = "UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = NULL && hasBeenRead = false"
            }

            // Within that transaction, update the inbox table for the received message to indicate that it has been read
            executeUpdate@TransactionService(updateRequest)()
            
            req.handle = tHandle

            // Call the corresponding operation at Service A
            finalizeChoreography@EmbedderInput(req.offset)
        }
    }
}