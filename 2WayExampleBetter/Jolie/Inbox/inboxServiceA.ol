from console import Console
from database import Database
from runtime import Runtime

from .inboxTypes import InboxEmbeddingConfig, InboxInterface
from ..serviceAInterface import ServiceAInterface
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
        Location: "socket://localhost:9090"
        Protocol: http{
            format = "json"
        }
        Interfaces: 
            ServiceAInterface
    }

    // Used when forwarding messages back to embedder
    outputPort Embedder {
        location: "local"   // Overwritten in init
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    // The TransactionService location needs to be the one initialized in ServiceA
    outputPort TransactionService {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: TransactionServiceInterface
    }

    embed Runtime as Runtime
    embed Console as Console
    embed Database as Database

    init
    {
        // This service takes control of receiving messages from external sources
        // It then needs to be able to forward the message to its embedder
        ExternalInput.location = p.externalLocation
        Embedder.location = p.localLocation

        // Make sure that the transaction service we're talking to is the same one as Service A.
        TransactionService.location = p.transactionServiceLocation

        // The inbox itself embeds MRS, which polls Kafka for updates
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime({
            filepath = "messageRetrieverService.ol"
            params << { 
                inboxServiceLocation << localLocation
                kafkaPollOptions << p.kafkaPollOptions
                kafkaInboxOptions << p.kafkaInboxOptions
            }
        })( )
        
        scope ( createtable ) 
        {
            connect@Database( p.databaseConnectionInfo )()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (request VARCHAR (150), hasBeenRead BOOLEAN, kafkaOffset INTEGER, rowid SERIAL PRIMARY KEY, UNIQUE(kafkaOffset));" )( ret )
        }
        println@Console( "InboxServiceA Initialized" )( )
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
            updateNumber@Embedder( req )( embedderResponse )
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
            finalizeChoreography@Embedder(req.offset)
        }
    }
}