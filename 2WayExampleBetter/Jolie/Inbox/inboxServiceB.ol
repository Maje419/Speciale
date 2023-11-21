from database import Database
from console import Console
from runtime import Runtime
from json-utils import JsonUtils
from reflection import reflection

from .inboxTypes import InboxEmbeddingConfig, InboxInterface
from ..TransactionService.transactionService import TransactionServiceInterface
from ..serviceBInterface import ServiceBInterface

service Inbox (p: InboxEmbeddingConfig){
    execution: concurrent
    // Used for embedded services to talk with the inbox
    inputPort InboxInput {
        Location: "local"
        Interfaces: 
            InboxInterface
    }

    // Used when forwarding messages back to embedder
    outputPort Embedder {
        location: "local"   // Overwritten in init
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    outputPort TransactionService {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: TransactionServiceInterface
    }

    embed Console as Console
    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Runtime as Runtime

    init
    {
        Embedder.location = p.localLocation
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

        // Create the inbox table
        scope ( createtable ) 
        {
            connect@Database( p.databaseConnectionInfo )()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (request VARCHAR (150), hasBeenRead BOOLEAN, kafkaOffset INTEGER, rowid SERIAL PRIMARY KEY, UNIQUE(kafkaOffset));" )( ret )
        }
        println@Console( "InboxServiceB Initialized" )( )
    }

    main{
        [recieveKafka( req )( res ) 
        {
            // Kafka messages for our inbox/outbox contains the operation invoked in the 'key', and the parameters in the 'value'
            scope( MakeIdempotent )
            {
                // Insert the request into the inbox table in the form:
                    // ______________________________________________________
                    // |            request          | hasBeenRead | offset |
                    // |———————————————————————————  |—————————————|————————|
                    // | operation:request           |   'false'   | offset |
                    // |————————————————————————————————————————————————————|

                install( SQLException => 
                {
                    println@Console("Message already recieved, commit request")();
                    res = "Message already recieveid, please re-commit"
                })
                
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
                .handle = tHandle;
                .update = "UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = " + req.offset + " AND hasBeenRead = false"
            }

            // Within that transaction, update the inbox table for the received message to indicate that it has been read
            executeUpdate@TransactionService(updateRequest)()
            
            getJsonValue@JsonUtils(req.value)(finalizeRequest)
            finalizeRequest.handle = tHandle

            // Call the corresponding operation at Service A
            with( embedderInvokationRequest ){
                .outputPort = "Embedder";
                .data << finalizeRequest;
                .operation = req.key
            }

            invoke@Reflection( embedderInvokationRequest )()
        }
    }
}