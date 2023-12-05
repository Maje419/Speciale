include "database.iol"
include "console.iol"
include "time.iol"
include "file.iol"

from runtime import Runtime
from .simple-kafka-connector import SimpleKafkaConnector

from ..test.testTypes import testParams, TestExceptionType

type UpdateNumberRequest {
    .username : string
}

type UpdateNumberResponse: string

interface ServiceAInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse ) throws TestException(TestExceptionType)
}

service ServiceA(p : testParams){
    execution: concurrent
    inputPort ServiceAExternal {
        location: "socket://localhost:8080"
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    inputPort ServiceATests {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    embed SimpleKafkaConnector as KafkaRelayer
    embed Runtime as Runtime

    init
    {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        };
        
        connect@Database( connectionInfo )( void );
        scope ( createTable ) 
        {
            install ( SQLException => println@Console("Table already exists")() );
            updateRequest =
                "CREATE TABLE IF NOT EXISTS NumbersA(username VARCHAR(50) NOT NULL, " +
                "number int)";
            update@Database( updateRequest )( ret )
        }
    }

    main {
        [ updateNumber( request )( response )
        {
            updateQuery = "UPDATE NumbersA SET number = number + 1 WHERE username = \"" + request.username + "\""

            // 0. No updates have yet happened in this service, so the two databases should still be synchronized
            if (p.throw_before_updating_local){
                throw ( TestException, "throw_before_updating" )
            }

            // 1: Service A updates its local state
            update@Database( updateQuery )( updateResponse )

            // 2. Service A is now out of sync with Service B, and has not yet sent an update to B  
            if (p.throw_before_sending){
                throw ( TestException, "throw_before_sending")
            }

            // 3: Propagate the updated username into Kafka
            propagateMessage@KafkaRelayer( request.username )
            
            if (p.throw_after_sending){
                throw ( TestException, "throw_after_sending" )
            }

            response = "Update succeded!"
        }]
    }
}
