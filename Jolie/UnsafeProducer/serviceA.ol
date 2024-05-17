from database import Database
from console import Console
from json-utils import JsonUtils
from time import Time

from .simple-kafka-connector import KafkaInserter

type StartChoreographyRequest {
    .username : string
}

type StartChoreographyResponse: string

interface ServiceAInterface{
    RequestResponse:
        startChoreography( StartChoreographyRequest )( StartChoreographyResponse )
}

service ServiceA{
    execution: concurrent
    inputPort ServiceAExternal {
        location: "socket://localhost:8080" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    embed JsonUtils as JsonUtils
    embed Console as Console
    embed Database as Database
    embed KafkaInserter as KafkaProducer
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
        update@Database( "CREATE TABLE IF NOT EXISTS customers(username TEXT NOT NULL, " +
            "dollarsSpent int)" )(  )
    }

    main {
        [ startChoreography( req )( res )
        {
            // ProducerRecievedTime
            getCurrentTimeMillis@Time( )( time )
            print@Console(" " + time)()

            // Check if the user exists, or if it needs to be created
            query@Database("SELECT * FROM customers WHERE username = '" + req.username + "';")( userExists )

           // Create user if needed, otherwise update the number for the user
            if (#userExists.row < 1){
                updateQuery = "INSERT INTO customers VALUES ('" + req.username + "', 0);"
            } else{
                updateQuery = "UPDATE customers SET dollarsSpent = dollarsSpent + 1 WHERE username = '" + req.username + "'"
            }

            // 1: Service A updates its local state
            update@Database( updateQuery )( updateResponse )

            //ProducerUpdateTime
            getCurrentTimeMillis@Time( )( time )
            print@Console(" " + time)()

            /***** If service crashes here, then the databases of the producer and consumer are inconsistant *****/

            /** A type repressenting a message to insert into kafka */
            with ( message ){
                .topic = "a-out"                                  
                .key = "react"                                   
                .brokerOptions << {
                    .topic = "a-out"                                 
                    .bootstrapServer = "localhost:29092"
                }
            }

            getJsonString@JsonUtils({.username = req.username})(params)
            getJsonString@JsonUtils({.mid = "123", .parameters = params} )(message.value)

            // 3: Propagate the updated username into Kafka
            propagateMessage@KafkaProducer( message )()
            res = "Choreography started!"
        }]
    }
}

