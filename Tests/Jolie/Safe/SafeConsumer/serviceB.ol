from .serviceBInterface import ServiceBInterface

from console import Console
from database import Database
from runtime import Runtime
from time import Time

from ..test.testTypes import ConsumerTestInterface

service ServiceB{
    execution: concurrent
    
    // Inputport used by the inbox
    inputPort ServiceBLocal {
        location: "local" 
        interfaces: ServiceBInterface, ConsumerTestInterface
    }

    outputPort IBOB {
        Location: "local"
        Interfaces: ConsumerTestInterface 
    }

    embed Database as Database
    embed Console as Console
    embed Runtime as Runtime
    embed Time as Time
    
    init {
        // Load the inbox service
        with ( connectionInfo ){
            .username = "postgres"
            .password = "example"
            .database = "service-b-db"
            .driver = "postgresql"
            .host = ""
        }

        getLocalLocation@Runtime()(localLocation)

        with ( inboxConfig ){
            .pollTimer = 1;
            .locations << {
                .localLocation << localLocation;
                .databaseServiceLocation << Database.location       
            }
            .kafkaOptions << {
                .pollAmount = 5
                .pollTimeout = 5000
                .bootstrapServer = "localhost:29092"
                .groupId = "service-b-consumer"
                .topic = "a-out"
            }
        }

        connect@Database( connectionInfo )( void )
        update@Database("CREATE TABLE IF NOT EXISTS users(username TEXT, balance int);")()

        loadEmbeddedService@Runtime({
            filepath = "InboxOutbox/ibob.ol"
            params << { 
                .inboxConfig << inboxConfig
                }
        })(IBOB.location)
    }

    main {
        [react( req )( res ){
            if (global.testParams.throw_on_message_received){
                global.hasThrownAfterForMessage = true
                throw ( TestException, "throw_on_message_received" )
            }

            // Check if the user exists, or if it needs to be created
            query@Database( {
                .query = "SELECT * FROM users WHERE username = '" + req.username + "';";
                .txHandle = req.txHandle
            } )( userExists )

           // Create user if needed, otherwise update the number for the user
            updateQuery.txHandle = req.txHandle
            if (#userExists.row < 1){
                updateQuery.update = "INSERT INTO users VALUES ('" + req.username + "', 100);"
            } else{
                updateQuery.update = "UPDATE users SET balance = balance - 1 WHERE username = '" + req.username + "'"
            }

            update@Database(updateQuery)()

            if (global.testParams.throw_after_local_update){
                global.hasThrownAfterForMessage = true
                throw ( TestException, "throw_after_local_update" )
            }

            // No need to commit here, this is handled by the inbox
            res = "Updated locally"
        }]

        [setupConsumerTests(req)(res){
            global.testParams << request.serviceB
            global.hasThrownAfterForMessage = false
            setupConsumerTests@IBOB(req)(res)
        }]
    }
}