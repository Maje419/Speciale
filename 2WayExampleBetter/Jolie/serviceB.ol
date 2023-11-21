from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

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
    embed JsonUtils as JsonUtils
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
                databaseConnectionInfo << config.serviceBConnectionInfo
                transactionServiceLocation << TransactionService.location   // All embedded services must talk to the same instance of 'TransactionServie'
                kafkaPollOptions << config.pollOptions
                kafkaInboxOptions << config.kafkaInboxOptions
            }
        } )( )

        // Load the outbox service as an embedded service
        loadEmbeddedService@Runtime( {
            filepath = "Outbox/outboxService.ol"
            params << { 
                pollSettings << config.pollOptions;
                databaseConnectionInfo << config.serviceBConnectionInfo;
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
        [numbersUpdated( req )]
        {   
            with ( userExistsQuery ){
                .query = "SELECT * FROM Numbers WHERE username = '" + req.username + "'";
                .handle = req.handle
            }
            executeQuery@TransactionService( userExistsQuery )( userExists )
                
            // Construct query which updates local state:
            if (#userExists.row < 1)
            {
                localUpdate.update = "INSERT INTO Numbers VALUES ('" + req.username + "', 0);"
            } 
            else
            {
                localUpdate.update = "UPDATE Numbers SET number = number + 1 WHERE username = '" + req.username + "'"
            }

            localUpdate.handle = req.handle

            executeUpdate@TransactionService( localUpdate )()

            with ( finalizeChoreographyRequest ){
                .username = req.username
            }
            with ( outboxQuery ){
                    .tHandle = req.handle;
                    .commitTransaction = true;
                    .topic = config.kafkaOutboxOptions.topic;
                    .operation = "finalizeChoreography"
            }
            getJsonString@JsonUtils( finalizeChoreographyRequest )( outboxQuery.data )

            println@Console("data: " + outboxQuery.data)()

            updateOutbox@OutboxService( outboxQuery )( updateResponse )
            println@Console("Service B has updated locally")()
        }
    }
}