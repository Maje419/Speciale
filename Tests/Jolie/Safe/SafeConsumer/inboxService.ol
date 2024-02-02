from .serviceBInterface import ServiceBInterface
from .inboxTypes import InboxEmbeddingConfig, InboxInterface
from .messageRetrieverService import MessageRetrieverInterface

from console import Console
from database import Database
from runtime import Runtime
from json_utils import JsonUtils

service Inbox (p: InboxEmbeddingConfig){
    execution: concurrent
    // Used for embedding services to talk with the inbox
    inputPort InboxInput {
        Location: "local"
        Interfaces: 
            InboxInterface
    }

    // Used when forwarding messages back to embedder
    outputPort EmbedderInput {
        location: "local"   // Overwritten in init
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    // Used during tests to setup test cases in the MRS
    outputPort MRS {
        Location: "local"  // Overwritten in init
        Interfaces: MessageRetrieverInterface
    }

    embed Console as Console
    embed Database as Database
    embed Runtime as Runtime
    embed JsonUtils as JsonUtils

    init
    {
        EmbedderInput.location = p.localLocation

        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "./SafeConsumer/messageRetrieverService.ol"
            params << {
                localLocation << localLocation
            }
        })( MRS.location )


        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }
        scope ( createtable ) 
        {
            connect@Database( connectionInfo )()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (request TEXT, hasBeenRead BOOLEAN, mid TEXT, UNIQUE(mid));" )( ret )
        }
        println@Console( "InboxServiceB Initialized" )(  )
    }

    main{
        [recieveKafka( req )( res ) {
            if (global.testParams.throw_on_message_found && !global.hasThrownAfterForMessage){
                global.hasThrownAfterForMessage = true
                throw ( TestException, "throw_on_message_found" )
            }
            // Kafka messages for our inbox/outbox contains the username in the 'key', and the parameters in the 'value'
            scope( MakeIdempotent ){
                // If this exception is thrown, its likely the unique constraint on the mid which failed
                install( SQLException => {
                    println@Console("Message already recieved, commit request")();
                    res = "Message already recieveid, please re-commit"
                })
                // Insert the request into the inbox table in the form:
                    // ___________________________________________________
                    // |            request        | hasBeenRead |  mid   |
                    // |———————————————————————————|—————————————|————————|
                    // |      'key': 'parameter(s)'|   'false'   |  uuid  |
                    // |——————————————————————————————————————————————————|
                    
                println@Console("Key: " + req.key + "\nValue: " + req.value)()

                // The Parameters and the mid is stored in a Json string, so construct the object
                getJsonValue@JsonUtils( req.value )( kafkaValue )

                if (global.testParams.throw_before_updating_inbox && !global.hasThrownAfterForMessage){
                    global.hasThrownAfterForMessage = true
                    throw ( TestException, "throw_before_updating_inbox" )
                }

                update@Database("INSERT INTO inbox (request, hasBeenRead, mid) VALUES (
                    \""+ req.key + ":" + kafkaValue.parameters +        // numbersUpdated:user1
                    "\", false, \"" +                         // false
                    kafkaValue.mid + "\");")()                      // offset
            }
            res = "Message stored"
        }] 
        {   
            if (global.testParams.throw_before_updating_main_service && !global.hasThrownAfterForMessage){
                global.hasThrownAfterForMessage = true
                throw ( TestException, "throw_before_updating_main_service" )
            }

            // In the future, we might use Reflection to hit the correct method in the embedder.
            updateUserRequest.userToUpdate = req.key
            updateNumberForUser@EmbedderInput( updateUserRequest )()
        }

        [setupTest( request )( response ){
            global.testParams << request.inboxTests
            global.hasThrownAfterForMessage = true
            setupTest@MRS( request.mrsTests )( response )
        }]
    }
}