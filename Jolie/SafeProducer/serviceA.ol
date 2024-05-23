from database import Database
from console import Console
from time import Time
from runtime import Runtime
from json-utils import JsonUtils


from ..InboxOutbox.publicOutboxTypes import OutboxInterface

type StartChoreographyRequest {
    .username : string
}

type StartChoreographyResponse: string


interface ServiceAInterface{
    RequestResponse:
        startChoreography( StartChoreographyRequest )( StartChoreographyResponse ),
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

    outputPort IBOB {
        Location: "local" //Overwritten in init
        Interfaces: OutboxInterface 
    }

    embed Console as Console
    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Runtime as Runtime
    embed Time as Time

    init
    {
        with ( connectionInfo ){
            .username = "postgres"
            .password = "example"
            .database = "service-a-db"
            .driver = "postgresql"
            .host = ""
        }
        
        connect@Database( connectionInfo )( void )
        update@Database( "CREATE TABLE IF NOT EXISTS Customers(username TEXT NOT NULL, " +
            "dollarsSpent int)" )( ret )

        // Configuration parameters needed for the outbox service(s)
        with ( outboxConfig ){
            .pollTimer = 1;
            .brokerOptions << {
                .topic = "a-out"
                .bootstrapServer = "localhost:29092"
            }
            .databaseServiceLocation << Database.location
        }

        loadEmbeddedService@Runtime({
            filepath = "InboxOutbox/ibob.ol"
            params << { 
                .outboxConfig << outboxConfig
                }
        })(IBOB.location)
    }

    main {
        [ startChoreography( req )( res )
        {
            // ProducerRecievedTime
            getCurrentTimeMillis@Time( )( time )
            print@Console(" " + time)()

            if (!is_defined(req.txHandle)){
                beginTx@Database()(req.txHandle)
                mustCommit = true
            }

            // Check if the user exists, or if it needs to be created
            query@Database( {
                .query = "SELECT * FROM Customers WHERE username = '" + req.username + "';";
                .txHandle = req.txHandle
            } )( userExists )

           // Create user if needed, otherwise update the number for the user
            updateQuery.txHandle = req.txHandle
            if (#userExists.row < 1){
                updateQuery.update = "INSERT INTO Customers VALUES ('" + req.username + "', 0);"
            } else{
                updateQuery.update = "UPDATE Customers SET dollarsSpent = dollarsSpent + 1 WHERE username = '" + req.username + "'"
            }

            update@Database( updateQuery )( updateResponse )
            
            // To update Service B, this service needs to communicate to its outbox that it should put a message
            // into kafka containing the operation and the request that we seek to call at                
            with ( outboxQuery ){
                .txHandle = req.txHandle;                               // The transaction handle for the ongoing transaction
                .topic = "a-out";                                       // .topic = "a-out", which is where service B inbox listens
                .operation = "react"                                    // This service wants to call serviceB.react
                .parameters << {.username = req.username}               // The request to send to the operation
            }
            
            updateOutbox@IBOB(outboxQuery)()
            if (mustCommit){    // Usually, the Inbox services would take care of committing, but these are not initialized in this example
                commitTx@Database(req.txHandle)()
                
                //ProducerUpdateTime
                getCurrentTimeMillis@Time( )( time )
                print@Console(" " + time)()
                
                mustCommit = false
            }
            res = "Choreography initiated"
        }]
    }
}

