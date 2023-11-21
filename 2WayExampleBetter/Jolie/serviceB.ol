include "serviceBInterface.iol"

from database import Database
from console import Console
from file import File
from runtime import Runtime

from .serviceBInterface import ServiceBInterface
from .Outbox.outboxService import OutboxInterface
from .TransactionService.transactionService import TransactionService

service ServiceB{
    execution: concurrent

    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    outputPort OutboxService {
        Location: "local"
        Protocol: http{
            format = "json"
        }
        Interfaces: OutboxInterface
    }

    embed Console as Console
    embed Database as Database
    embed File as File
    embed Runtime as Runtime
    embed TransactionService as TransactionService

    init {

        readFile@File(
            {
                filename = "serviceBConfig.json"
                format = "json"
            }) ( config )

        getLocalLocation@Runtime()( location )
        loadEmbeddedService@Runtime( { 
            filepath = "Inbox/inboxServiceB.ol"
            params << { 
                localLocation << location
                externalLocation << "socket://localhost:8082"       //This doesn't work for some reason
                configFile = "serviceBConfig.json"
            }
        } )( )

        // Load the outbox service as an embedded service
        loadEmbeddedService@Runtime( {
            filepath = "Outbox/outboxService.ol"
            params << { 
                pollSettings << config.pollOptions;
                databaseConnectionInfo << config.serviceAConnectionInfo;
                brokerOptions << config.kafkaOutboxOptions;
                transactionServiceLocation << TransactionService.location
            }
        } )( OutboxService.location )    // It is very important that this is a lower-case 'location', otherwise it doesn't work
                                        // Guess how long it took me to figure that out :)

        connect@TransactionService(config.serviceBConnectionInfo)()
        connect@Database( config.serviceBConnectionInfo )( )

        scope ( createtable ) 
        {
            updaterequest = "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)"
            update@Database( updaterequest )( ret )
        }
    }

    main 
    {
        [numbersUpdated( request )]
        {
            println@Console("ServiceB: \t numbersUpdated called with username " + req.username)()

            scope (ForceSequentialDatabaseAccess)
            {
                connect@Database( config.serviceBConnectionInfo )( void )
                query@Database( "SELECT * FROM inbox WHERE hasBeenRead = false" )( inboxMessages )
            }

            for ( row in inboxMessages.row ) 
            {
                println@Console("ServiceB: Checking inbox found update request " + row.request)()
                row.request.regex = ":"
                split@StringUtils( row.request )( splitResult )
                username = splitResult.result[1]
                query@Database("SELECT * FROM Numbers WHERE username = \"" + username + "\"")( userExists )
                
                // Construct query which updates local state:
                if (#userExists.row < 1)
                {
                    localUpdateQuery = "INSERT INTO Numbers VALUES (\"" + username + "\", 0);"
                } 
                else
                {
                    localUpdateQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + username + "\""
                }

                // Construct query to delete message row from inbox table
                inboxDeleteQuery = "UPDATE inbox SET hasBeenRead = true WHERE rowid = " + row.rowid
                //TODO: The above query does not consider that kafkaOffset can be NULL for messages not coming out of Kafka.
                        // In this case, it doesn't matter, since all messages come from kafka, but in a more advanced example
                        // it might be a problem

                scope (ExecuteQueries)
                {
                    updateQuery.sqlQuery[0] = localUpdateQuery
                    updateQuery.sqlQuery[1] = inboxDeleteQuery
                
                    updateQuery.topic = config.kafkaOutboxOptions.topic
                    updateQuery.key = "numbersUpdated"
                    updateQuery.value = username
                    transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                    println@Console("Service B has updated locally")()
                }
            }
        }
    }
}