include "database.iol"
include "time.iol"
include "console.iol"
include "string_utils.iol"
include "serviceBInterface.iol"

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
                transaction[0] = "INSERT INTO example VALUES (\"" + row.request + "\");"
                transaction[1] = "UPDATE example SET hasBeenRead = true WHERE kafkaOffset = " + req.offset + ";"
                executeTransaction@Database( transaction )(  )
                update@Database( "INSERT INTO example VALUES (\"" + row.request + "\");" )(  )
            }
            println@Console( "SErviceA: Handling request for username " + request.userToUpdate )()
            response.code = 200
            response.reason = "Updated locally"
        }]
    }
}