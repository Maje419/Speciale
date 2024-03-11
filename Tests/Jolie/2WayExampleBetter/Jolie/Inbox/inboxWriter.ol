from console import Console
from database import Database
from runtime import Runtime
from json-utils import JsonUtils
from reflection import Reflection
from time import Time

from .inboxTypes import InboxEmbeddingConfig, KafkaMessage, InboxWriterInsertRequest
from ...test.testTypes import TestInterface

// This interface is used by the MRS to call the InboxWriter when it finds a new message in Kafka
interface InboxWriterKafkaInterface {               
    RequestResponse: 
        recieveKafka( KafkaMessage )( string )
}

// This interface can be called by the user to insert a new message in the inbox table
interface InboxWriterExternalInterface {
    RequestResponse:
        insertIntoInbox( InboxWriterInsertRequest )( string )
}

/**
*   This service handles receiving messages, and inserting them into the 'inbox' table.
*       Inbox table:
*           ____________________________________________________________________________
*           |       operation      |       parameters      |   arrivedFromKafka  |  messageId  |
*           |——————————————————————|———————————————————————|—————————————————————|—————————————|
*           |     "updateNumber"   |  {"username":"user1"} |           T         |    42       |
*           |——————————————————————|———————————————————————|—————————————————————|—————————————|
*           |     "finalizeChor"   |  {"username":"user3"} |           F         |    42       |
*           |——————————————————————|———————————————————————|—————————————————————|—————————————|
*/
service InboxWriterService (p: InboxEmbeddingConfig){
    execution: concurrent

    // Used for MRS to talk with the inbox
    inputPort InboxInput {
        Location: "local"
        Interfaces: 
            InboxWriterKafkaInterface,
            TestInterface
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

    outputPort MRS {
        Location: "local"       // Overwritten in init
        Interfaces: TestInterface
    }

    embed Console as Console
    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Runtime as Runtime
    embed Time as Time

    init
    {
        connect@Database( p.databaseConnectionInfo )()
        // This query uses a mix of arrivedFromKafka and mid, since we now need to recieve messages from outside Kafka as well.
        // If we used the unique constraint only on the mid, we might reach a situation where some ID generated by the 
        // Outbox of Service A was already assigned to some message that Service B had recieved from another source.
        // Another issue arrises if Service B recieves messages on different topics, but in such a case, it would
        // suffice to introduce the topic as a part of the unique constraint.
        update@Database( 
            "CREATE TABLE IF NOT EXISTS inbox (operation VARCHAR (150), parameters TEXT, arrivedFromKafka BOOL, messageId TEXT, UNIQUE(arrivedFromKafka, messageId));" 
            )( ret )

        // This service handles inserting messages into the 'Inbox' table. Messages can be recieved from a socket location, or from Kafka.
        // Messages from Kafka are read by the MRS service, instantiated here. MRS will forward new messages from Kafka and int this service.
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime({
            filepath = "Jolie/Inbox/messageRetrieverService.ol"
            params << 
            { 
                inboxServiceLocation << localLocation
                kafkaPollOptions << p.kafkaPollOptions
                kafkaInboxOptions << p.kafkaInboxOptions
            }
        })( MRS.location )
        
        println@Console( "InboxWriter Initialized at location '" + localLocation + "'" )( )
        println@Console( "MRS Initialized at location '" + MRS.location + "'" )( )
    }

    main{
        [insertIntoInbox( req )( res ){
            scope( MakeIdempotent )
            {
                //install( SQLException => res = "Problem inserting the message")

                getJsonString@JsonUtils( req.request )( inboxRequest )

                if (global.testParams.insert_called_throw_before_update_local){
                    throw (TestException, "insert_called_throw_before_update_local")
                }
                
                // If an Id is provided, we can assure exactly-once-delivery even at this step
                if ( req.request.id instanceof string )
                {
                    update@Database("INSERT INTO inbox (operation, parameters, arrivedFromKafka, messageId) VALUES ('" + req.operation + "','" + inboxRequest + "', false, '" + req.id + "');")()
                } 
                else // req.request.id instanceof void
                {
                    getProcessId@Runtime( )( processId )
                    getCurrentTimeMillis@Time()( timeMillis )
                    messageId = processId + ":" + timeMillis
                    update@Database("INSERT INTO inbox (operation, parameters, arrivedFromKafka, messageId) VALUES ('" + req.operation + "','" + inboxRequest + "', false, '" + messageId + "');")()
                }
            }
        }]

        [recieveKafka( req )( res ) 
        {
            scope( MakeIdempotent )
            {
                install( SQLException => res = "Message already recieved" )

                getJsonValue@JsonUtils( req.value )( kafkaValue )

                getJsonString@JsonUtils(kafkaValue.parameters)(parametersString)

                if (global.testParams.recieve_called_throw_before_update_local){
                    global.hasThrown = true
                    throw (TestException, "recieve_called_throw_before_update_local")
                }
                
                // As per protocol, the key will be the operation, and the value a string containing the request. 
                update@Database("INSERT INTO inbox (operation, parameters, arrivedFromKafka, messageId) VALUES ('" + req.key + "','" + parametersString + "', true, '" + kafkaValue.mid  + "');")()
                
                res = "Message stored"
            }
        }]

        [setupTest( request )( response ){
            println@Console("InboxWriter tests")()

            global.testParams << request.inboxWriterTests
            global.hasThrown = false
            setupTest@MRS(request)(response)
        }]
    }
}