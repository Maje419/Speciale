from .serviceBInterface import ServiceBInterface

from console import Console
from database import Database
from runtime import Runtime

service ServiceB{
    execution: sequential
    
    // Inputport used by the inbox
    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
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
        } )( _ )

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
            query@Database( "SELECT * FROM inbox WHERE hasBeenRead = false" )( inboxMessages )

            for ( row in inboxMessages.row ) 
            {
                println@Console("ServiceB: Checking inbox found update request " + row.request)()
                transaction.statement[0] = "INSERT INTO example VALUES (\"" + row.request + "\");"
                transaction.statement[1] = "UPDATE inbox SET hasBeenRead = true WHERE mid = \"" + row.mid + "\";"

                println@Console(transaction.statement[0])()
                println@Console(transaction.statement[1])()
                executeTransaction@Database( transaction )(  )
            }
            response.code = 200
            response.reason = "Updated locally"
        }]
    }
}