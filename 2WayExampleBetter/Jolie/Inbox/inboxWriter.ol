from console import Console
from database import Database
from runtime import Runtime
from json-utils import JsonUtils
from reflection import Reflection

from .inboxTypes import InboxEmbeddingConfig, KafkaMessage, InboxWriterInsertRequest

// This interface is used by the MRS to call the InboxWriter when it finds a new message in Kafka
interface InboxWriterKafkaInterface {               
    RequestResponse: 
        recieveKafka( KafkaMessage )( string )
}

// This interface can be called by the user to insert a new message in the inbox table
interface InboxWriterExternalInterface {
    OneWay:
        insertIntoInbox( InboxWriterInsertRequest )
}

/**
*   This service handles receiving messages, and inserting them into the 'inbox' table.
*       Inbox table:
*           ____________________________________________________________________________
*           |       operation      |       parameters      |   kafakaId  |  messageId  |
*           |——————————————————————|———————————————————————|—————————————|—————————————|
*           |     "updateNumber"   |  {"username":"user1"} |     NULL    |    42       |
*           |——————————————————————|———————————————————————|—————————————|—————————————|
*           |     "finalizeChor"   |  {"username":"user3"} |     1337    |    42       |
*           |——————————————————————|———————————————————————|—————————————|—————————————|
*/
service InboxWriterService (p: InboxEmbeddingConfig){
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
    embed Runtime as Runtime

    init
    {
        connect@Database( p.databaseConnectionInfo )()
        update@Database( "CREATE TABLE IF NOT EXISTS inbox (operation VARCHAR (150), parameters TEXT, kafkaId INTEGER, messageId INTEGER GENERATED BY DEFAULT AS IDENTITY, UNIQUE(kafkaId, messageId));" )( ret )

        // This service handles inserting messages into the 'Inbox' table. Messages can be recieved from a socket location, or from Kafka.
        // Messages from Kafka are read by the MRS service, instantiated here. MRS will forward new messages from Kafka and int this service.
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime({
            filepath = "Inbox/messageRetrieverService.ol"
            params << 
            { 
                inboxServiceLocation << localLocation
                kafkaPollOptions << p.kafkaPollOptions
                kafkaInboxOptions << p.kafkaInboxOptions
            }
        })( )
        
        println@Console( "InboxWriter Initialized at location '" + localLocation + "'" )( )
    }

    main{
        [insertIntoInbox( req )]{
            scope( MakeIdempotent )
            {
                install( SQLException => println@Console("Message already recieved")() )

                getJsonString@JsonUtils( req.request )( parameters )

                // If an Id is provided, we can assure exactly-once-delivery even at this step
                if ( is_defined( req.id ))
                {
                    update@Database("INSERT INTO inbox (operation, parameters, kafkaId, messageId) VALUES ('" + req.operation + "','" + parameters + "', -1, " + req.id + ");")()
                } 
                else 
                {
                    update@Database("INSERT INTO inbox (operation, parameters, kafkaId) VALUES ('" + req.operation + "','" + parameters + "', -1);")()
                }
            }
            res << "Message stored"
        }

        [recieveKafka( req )( res ) 
        {
            scope( MakeIdempotent )
            {
                install( SQLException => println@Console("Message already recieved")() )
                
                // As per protocol, the key will be the operation, and the value a string containing the request. 
                update@Database("INSERT INTO inbox (operation, parameters, kafkaId) VALUES ('" + req.key + "','" + req.value + "', " + req.offset + ");")()
            }
            res << "Message stored"
        }]
    }
}