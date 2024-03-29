from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

from .serviceCInterface import ServiceCInterface
from ..Outbox.outboxService import OutboxInterface
from ..TransactionService.transactionService import TransactionService

service ServiceC{
    execution: concurrent

    inputPort ServiceCLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceCInterface
    }

    outputPort IBOB {
        Location: "local"
        protocol: http{
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

    init{
        readFile@File(
            {
                filename = "ServiceC/serviceCConfig.json"
                format = "json"
            }) ( config )

        getLocalLocation@Runtime()( location )

        with( inboxConfig )
        { 
            .localLocation << location;
            .externalLocation << "socket://localhost:8080";       //This doesn't work (yet) - Idea was to be able to "hand over" control of taking incomming messages in a generic way
            .databaseConnectionInfo << config.serviceCConnectionInfo;
            .transactionServiceLocation << TransactionService.location;   // All embedded services must talk to the same instance of 'TransactionServie'
            .kafkaPollOptions << config.pollOptions;
            .kafkaInboxOptions << config.kafkaInboxOptions
        }

        with ( outboxConfig ){
            .pollSettings << config.pollOptions;
            .databaseConnectionInfo << config.serviceCConnectionInfo;
            .brokerOptions << config.kafkaOutboxOptions;
            .transactionServiceLocation << TransactionService.location
        }

        loadEmbeddedService@Runtime({
            filepath = "InboxOutbox/ibob.ol"
            params << { 
                .inboxConfig << inboxConfig;
                .outboxConfig << outboxConfig
                }
        })(IBOB.location)
        inboxConfig.ibobLocation = IBOB.location

        connect@TransactionService(config.serviceCConnectionInfo)()
        connect@Database( config.serviceCConnectionInfo )( )

        scope ( createtable ) 
        {
            updaterequest = "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)"
            update@Database( updaterequest )( ret )
        }
    }

    main
    {
        [updateNumbers( req )( res ){
            // Once a request is recieved, we update our own local state for that person
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

            // Send to our outbox that we've completed our update
            with ( finalizeServiceARequest ){
                .username = req.username
            }

            with ( outboxQuery ){
                    .tHandle = req.handle;
                    .topic = config.kafkaOutboxOptions.topic;
                    .operation = "finalizeChoreography"
            }
            getJsonString@JsonUtils( finalizeServiceARequest )( outboxQuery.data )

            updateOutbox@IBOB( outboxQuery )( updateResponse )
            
            res = true
            println@Console("Service C has updated locally")()
        }]
    }
}