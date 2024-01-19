include "database.iol"
include "console.iol"
include "time.iol"

from .outboxService import Outbox

type UpdateNumberRequest {
    .username : string
}

type UpdateNumberResponse: string

interface ServiceAInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse )
}

service ServiceA{
    execution: concurrent
    inputPort ServiceA {
        location: "socket://localhost:8080" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }
    embed Outbox as OutboxService

    init
    {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }
        
        with ( pollSettings )
        {
            .pollAmount = 3
            .pollDurationMS = 3000
        }

        with ( kafkaOptions )
        {
            .bootstrapServers =  "localhost:9092"
            .groupId = "test-group"
        }

        with ( outboxSettings )
        {
            .pollSettings << pollSettings;
            .databaseConnectionInfo << connectionInfo;
            .brokerOptions << kafkaOptions
        }
        
        connectKafka@OutboxService( outboxSettings ) ( outboxResponse )
        connect@Database( connectionInfo )( void )

        scope ( createTable ) 
        {
            install ( SQLException => println@Console("Numbers table already exists")() )
            updateRequest =
                "CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, " +
                "number int)";
            update@Database( updateRequest )( ret )
        }
    }

    main {
        [ updateNumber( request )( response )
        {
            println@Console("UpdateNumber called with username " + request.username)()
            scope ( InsertData )    //Update the number in the database
            {   
                install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )
                updateQuery.sqlQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + request.username + "\""
                updateQuery.topic = "example"
                updateQuery.key = request.username
                updateQuery.value = "Updated number for " + request.username
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                println@Console( "Update response: " + updateResponse )(  )
                response = "Choreography Started!"
            }
        }]
    }
}

