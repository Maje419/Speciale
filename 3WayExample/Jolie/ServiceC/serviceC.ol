from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

from .serviceCInterface import ServiceCInterface
from ..Outbox.outboxService import OutboxInterface

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

    init{
        readFile@File(
            {
                filename = "ServiceC/serviceCConfig.json"
                format = "json"
            }) ( config )

        connect@Database( config.serviceCConnectionInfo )( )
        update@Database( "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)" )( ret )

        getLocalLocation@Runtime()( location )

        with( inboxConfig )
        { 
            .localLocation << location;
            .externalLocation << "socket://localhost:8080";       //This doesn't work (yet) - Idea was to be able to "hand over" control of taking incomming messages in a generic way
            .databaseConnectionInfo << config.serviceCConnectionInfo;
            .databaseServiceLocation << Database.location;   // All embedded services must talk to the same instance of 'TransactionServie'
            .kafkaPollOptions << config.pollOptions;
            .kafkaInboxOptions << config.kafkaInboxOptions
        }

        with ( outboxConfig ){
            .pollSettings << config.pollOptions;
            .databaseConnectionInfo << config.serviceCConnectionInfo;
            .brokerOptions << config.kafkaOutboxOptions;
            .databaseServiceLocation << Database.location
        }

        loadEmbeddedService@Runtime({
            filepath = "InboxOutbox/ibob.ol"
            params << { 
                .inboxConfig << inboxConfig;
                .outboxConfig << outboxConfig
                }
        })(IBOB.location)
    }

    main
    {
        [react( req )( res ){
            println@Console("Recieved message for operation react!")()
            // Once a request is recieved, we update our own local state for that person
            with ( userExistsQuery ){
                .query = "SELECT * FROM Numbers WHERE username = '" + req.username + "'";
                .txHandle = req.txHandle
            }
            query@Database( userExistsQuery )( userExists )
                
            // Construct query which updates local state:
            if (#userExists.row < 1)
            {
                localUpdate.update = "INSERT INTO Numbers VALUES ('" + req.username + "', 0);"
            } 
            else
            {
                localUpdate.update = "UPDATE Numbers SET number = number + 1 WHERE username = '" + req.username + "'"
            }
            
            localUpdate.txHandle = req.txHandle
            update@Database( localUpdate )()

            // Send to our outbox that we've completed our update
            with ( finalizeServiceARequest ){
                .username = req.username
            }

            with ( outboxQuery ){
                    .txHandle = req.txHandle;
                    .topic = config.kafkaOutboxOptions.topic;
                    .operation = "finalizeChoreography"
            }
            getJsonString@JsonUtils( finalizeServiceARequest )( outboxQuery.data )

            updateOutbox@IBOB( outboxQuery )( updateResponse )
            
            res = true
        }]
    }
}