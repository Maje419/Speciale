from .serviceBInterface import ServiceBInterface
from .messageRecieverService import MRS

from console import Console
from database import Database
from runtime import Runtime
from time import Time

service ServiceB{
    execution: concurrent
    
    // Inputport used by the inbox
    inputPort ServiceBLocal {
        location: "local" 
        interfaces: ServiceBInterface
    }

    embed Console as Console
    embed Database as Database
    embed Runtime as Runtime
    embed Time as Time
    
    init {
        with ( connectionInfo ){
            .username = "postgres"
            .password = "example"
            .database = "service-b-db"
            .driver = "postgresql"
            .host = ""
        }

        connect@Database( connectionInfo )( void )
        update@Database("CREATE TABLE IF NOT EXISTS users(username TEXT, balance int);")()

        getLocalLocation@Runtime()(location)
        loadEmbeddedService@Runtime({
            filepath = "UnsafeConsumer/messageRecieverService.ol"
            params << { 
                        .kafkaOptions << {
                            .pollAmount = 5
                            .pollTimeout = 5000
                            .bootstrapServer = "localhost:29092"
                            .groupId = "service-b-consumer"
                            .topic = "a-out"
                        }
                        .mainServiceLocation << location
                    }
        })()
    }

    main {
        [react( req )( res ){
            // Check if the user exists, or if it needs to be created
            query@Database( "SELECT * FROM users WHERE username = '" + req.username + "';" )( userExists )

           // Create user if needed, otherwise update the number for the user
            if (#userExists.row < 1){
                updateQuery = "INSERT INTO users VALUES ('" + req.username + "', 100);"
            } else{
                updateQuery = "UPDATE users SET balance = balance - 1 WHERE username = '" + req.username + "'"
            }

            //ConsumerUpdateTime
            getCurrentTimeMillis@Time()( time )
            print@Console(" " + time)()
            update@Database(updateQuery)()

            res = "Updated locally"
        }]
    }
}