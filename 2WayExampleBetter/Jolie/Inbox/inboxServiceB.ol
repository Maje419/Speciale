include "Inbox/inboxTypes.iol"
include "serviceBInterface.iol"

from database import Database
from console import Console
from runtime import Runtime

from .Inbox.inboxTypes import InboxEmbeddingConfig, InboxInterface
from .serviceBInterface import ServiceBInterface

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
        Location: "local"   // Overwtitten in init
        Protocol: 
        Interfaces: 
    }
    embed Console as Console
    embed Database as Database
    embed Runtime as Runtime

    init
    {
        Embedder.location = p.localLocation

        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "messageRetrieverService.ol"
            params << {
                localLocation << localLocation
                configFile = p.configFile
            }
        })( MessageRetriever.location )

        readFile@File(
            {
                filename = p.configFile
                format = "json"
            }) ( config )

        scope ( createtable ) 
        {
            connect@Database( config.serviceBConnectionInfo )()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (request VARCHAR (150), hasBeenRead BOOLEAN, kafkaOffset INTEGER, rowid INTEGER PRIMARY KEY AUTOINCREMENT, UNIQUE(kafkaOffset));" )( ret )
        }
        println@Console( "InboxServiceB Initialized" )(  )
    }

    main{
        [RecieveKafka( req )( res ) {
            // Kafka messages for our inbox/outbox contains the operation invoked in the 'key', and the parameters in the 'value'
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
                    \""+ req.key + ":" + req.value +        // numbersUpdated:user1
                    "\", false, " +                         // false
                    req.offset + ")")()                      // offset

                
            }
            res << "Message stored"
        }] 
        {   
            initializeTransaction@TransactionService()(tHandle)

            with (updateRequest){
                .handle = tHandle
                .update = "UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = NULL && hasBeenRead = false"
            }

            // Within that transaction, update the inbox table for the received message to indicate that it has been read
            executeUpdate@TransactionService(updateRequest)()
            
            req.handle = tHandle
            // In the future, we might use Reflection to hit the correct method in the embedder.
            numbersUpdated@Embedder( "Nice" )
        }
    }
}