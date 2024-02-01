from .serviceBInterface import ServiceBInterface
from .inboxTypes import InboxInterface

from console import Console
from database import Database
from runtime import Runtime

service ServiceB{
    execution: concurrent
    
    // Inputport used by the inbox
    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    // This OPP is here for setting up tests in the inbox
    outputPort InboxService {
        Location: "local"   //overwritten in init
        Interfaces:  InboxInterface
    }

    embed Database as Database
    embed Console as Console
    embed Runtime as Runtime
    
    init {
        // Load the inbox service
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime( { 
            filepath = "inboxService.ol"
            params << { 
                localLocation << localLocation
                externalLocation << "socket://localhost:8082"
            }
        } )( InboxService.location )

        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        connect@Database( connectionInfo )( void )
        update@Database("CREATE TABLE IF NOT EXISTS example(message VARCHAR(100));")()
    }

    main {
        [updateNumberForUser( request )( response ){
            if (global.testParams.throw_before_search_inbox){
                throw ( TestException, "throw_before_search_inbox" )
            }
            query@Database( "SELECT * FROM inbox WHERE hasBeenRead = false" )( inboxMessages )

            for ( row in inboxMessages.row ) 
            {
                if (global.testParams.throw_at_inbox_message_found && !global.hasThrownAfterForMessage){
                    // Ensure that for testing, this method only 'crashes' the first time for some message
                    global.hasThrownAfterForMessage = true
                    throw ( TestException, "throw_at_inbox_message_found" )
                }

                transaction.statement[0] = "INSERT INTO example VALUES (\"" + row.request + "\");"
                transaction.statement[1] = "UPDATE inbox SET hasBeenRead = true WHERE mid = \"" + row.mid + "\";"

                println@Console(transaction.statement[0])()
                println@Console(transaction.statement[1])()
                executeTransaction@Database( transaction )(  )
                if (global.testParams.throw_after_transaction){
                    throw ( TestException, "throw_after_transaction" )
                }

            }
            response.code = 200
            response.reason = "Updated locally"
        }]

        [setupTest( request )( response ){
            global.testParams << request.ServiceBTests
            setupTest@InboxService(request.inboxTests)(response)
        }]
    }
}