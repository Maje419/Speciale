from database import Database, ConnectionInfo
from console import Console
from time import Time

from .outboxService import Outbox
from ..test.testTypes import ConsumerTests, TestExceptionType

type UpdateNumberRequest {
    .username : string
}

type UpdateNumberResponse: string


interface ServiceAInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse ) throws TestException(TestExceptionType),
        setupTest( ConsumerTests )( bool ) 
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

    // Here for tests
    inputPort ServiceALocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    embed Outbox as OutboxService
    embed Database as Database
    embed Time as Time
    embed Console as Console

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
            .pollDurationMS = 500
        }

        with ( kafkaOptions )
        {
            .bootstrapServers =  "localhost:29092"
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
        [setupTest( request )( response ){
            println@Console("Setting up tests in serviceA")()
            global.testParams << request.serviceA
            setupTest@OutboxService( request )( response )
        }]
        
        [ updateNumber( request )( response )
        {
            scope ( InsertData )    //Update the number in the database
            {   
                install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )
                updateQuery.sqlQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + request.username + "\""
                updateQuery.topic = "example"
                updateQuery.key = request.username
                updateQuery.value = "Updated number for " + request.username

                // If thrown here, the outbox will never be updated, but neither is the local state, so services are consistant
                if (global.testParams.throw_before_outbox_call){
                    throw ( TestException, "throw_before_outbox_call" )
                }
                
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                
                // If thrown here, outbox is already updated, so eventual consistency is achieved
                if (global.testParams.throw_after_outbox_call){
                    throw ( TestException, "throw_after_outbox_call" )
                }

                response = "Choreography Started!"
            }
        }]
    }
}

