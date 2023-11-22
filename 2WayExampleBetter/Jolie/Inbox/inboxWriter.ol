from console import Console
from database import Database
from runtime import Runtime
from json-utils import JsonUtils
from reflection import Reflection

from .inboxTypes import InboxEmbeddingConfig, InboxWriterKafkaInterface, InboxWriterExternalInterface
from ..serviceAInterface import ServiceAInterface
from ..TransactionService.transactionService import TransactionServiceInterface


/**
*   This service handles receiving messages, and inserting them into the 'inbox' table.
*       Inbox table:
*           _____________________________________________________________________________________________
*           |       operation      |       parameters      | hasBeenRead |   kafakaId  |  messageId  |
*           |——————————————————————|———————————————————————|—————————————|—————————————|—————————————|
*           |     "updateNumber"   |  {"username":"user1"} |      f      |     NULL    |    42       |
*           |——————————————————————|———————————————————————|—————————————|—————————————|—————————————|
*           |     "finalizeChor"   |  {"username":"user3"} |      y      |     1337    |    42       |
*           |——————————————————————|———————————————————————|—————————————|—————————————|—————————————|
*/
service InboxWriter (p: InboxEmbeddingConfig){
    execution: concurrent

    // Used for MRS to talk with the inbox
    inputPort InboxInput {
        Location: "local"
        Interfaces: 
            InboxWriterKafkaInterface
    }

    // This port can be called by an embedder to manually insert a new message
    inputPort ExternalInput {
        Location: "local"
        Protocol: http{
            format = "json"
        }
        Interfaces: 
            InboxWriterExternalInterface
    }

    embed Console as Console
    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Reflection as Reflection
    embed Runtime as Runtime

    init
    {
        // This service handles inserting messages into the 'Inbox' table. Messages can be recieved from a socket location, or from Kafka.
        // Messages from Kafka are read by the MRS service, instantiated here. MRS will forward new messages from Kafka and int this service.
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime({
            filepath = "messageRetrieverService.ol"
            params << 
            { 
                inboxServiceLocation << localLocation
                kafkaPollOptions << p.kafkaPollOptions
                kafkaInboxOptions << p.kafkaInboxOptions
            }
        })( )
        
        scope ( createtable ) 
        {
            connect@Database( p.databaseConnectionInfo )()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (operation VARCHAR (150), parameters TEXT, hasBeenRead BOOLEAN, kafkaId INTEGER, messageId INTEGER GENERATED BY DEFAULT AS IDENTITY, UNIQUE(kafkaId, messageId));;" )( ret )
        }
        println@Console( "InboxWriter Initialized at location '" + localLocation + "'" )( )
    }

    main{
        [receiveExternal( req )( res ){
            scope( MakeIdempotent )
            {
                install( SQLException => println@Console("Message already recieved")() )

                // Get a string representation of the request
                getJsonString@JsonUtils( req.request )( parameters )

                // If an Id is provided, we can assure exactly-once-delivery even at this step
                if ( is_defined( req.id ))
                {
                    update@Database("INSERT INTO inbox (operation, parameters, hasbeenread, kafkaId, messageId) VALUES (' " + req.operation "','" + parameters + "', false, -1, " + req.id + ");")()
                } 
                else 
                {
                    update@Database("INSERT INTO inbox (operation, parameters, hasbeenread, kafkaId) VALUES (' " + req.operation "','" + parameters + "', false, -1);")()
                }
            }
            res << "Message stored"
        }]

        [recieveKafka( req )( res ) 
        {
            scope( MakeIdempotent )
            {
                install( SQLException => println@Console("Message already recieved")() )
                
                // As per protocol, the key will be the operation, and the value a string containing the request. 
                update@Database("INSERT INTO inbox (operation, parameters, hasbeenread, kafkaId) VALUES (' " + req.key + "','" + req.value + "', false, " + req.offset + ");")()
            }
            res << "Message stored"
        }]
        // {   
        //     // Initialize a new transaction to pass onto Service A
        //     initializeTransaction@TransactionService()(tHandle)

        //     with (updateRequest){
        //         .handle = tHandle;
        //         .update = "UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = " + req.offset + " AND hasBeenRead = false"
        //     }

        //     // Within that transaction, update the inbox table for the received message to indicate that it has been read
        //     executeUpdate@TransactionService(updateRequest)()
            
        //     getJsonValue@JsonUtils(req.value)(finalizeRequest)
        //     finalizeRequest.handle = tHandle

        //     // Call the corresponding operation at Service A
        //     with( embedderInvokationRequest ){
        //         .outputPort = "Embedder";
        //         .data << finalizeRequest;
        //         .operation = req.key
        //     }

        //     invoke@Reflection( embedderInvokationRequest )()
        // }
    }
}