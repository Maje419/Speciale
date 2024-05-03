from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

from .serviceBInterface import ServiceBInterface
from ..Outbox.outboxService import OutboxInterface

service ServiceB{
    execution: concurrent

    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    outputPort IBOB {
        Location: "local"
        Interfaces: OutboxInterface
    }

    embed Console as Console
    embed Database as Database
    embed File as File
    embed JsonUtils as JsonUtils
    embed Runtime as Runtime

    init {
        readFile@File(
            {
                filename = "ServiceB/serviceBConfig.json"
                format = "json"
            }) ( config )

        getLocalLocation@Runtime()( location )

        with( inboxConfig )
        { 
            .localLocation << location;
            .databaseConnectionInfo << config.serviceBConnectionInfo;
            .databaseServiceLocation << Database.location;
            .kafkaPollOptions << config.pollOptions;
            .kafkaInboxOptions << config.kafkaInboxOptions
        }

        with ( outboxConfig ){
            .pollSettings << config.pollOptions;
            .databaseConnectionInfo << config.serviceBConnectionInfo;
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

        connect@Database( config.serviceBConnectionInfo )( )

        scope ( createtable ) 
        {
            update@Database( "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)" )( ret )
        }
    }

    main 
    {
        [react( req )( res )
        {   
            scope (ExecuteLocalupdate) {
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
            }

            scope (UpdateOutbox){
                with ( updateServiceCRequest ){
                    .username = req.username
                }
                with ( outboxQuery ){
                        .txHandle = req.txHandle;
                        .topic = config.kafkaOutboxOptions.topic;
                        .operation = "finalizeChoreography"
                }
                
                getJsonString@JsonUtils( updateServiceCRequest )( outboxQuery.data )

                updateOutbox@IBOB( outboxQuery )( updateResponse )
                println@Console("Service B has updated locally")()
            }
            res = "Service B has updated locally"
        }]
    }
}