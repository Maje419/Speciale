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

    init {
        readFile@File(
            {
                filename = "ServiceB/serviceBConfig.json"
                format = "json"
            }) ( config )

        connect@Database( config.serviceBConnectionInfo )( )
        update@Database( "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)" )( ret )
        
        getLocalLocation@Runtime()( location )
        with( inboxConfig )
        { 
            .localLocation << location;
            .externalLocation << "socket://localhost:8080";       //This doesn't work (yet)
            .databaseConnectionInfo << config.serviceBConnectionInfo;
            .databaseServiceLocation << Database.location;   // All embedded services must talk to the same instance of 'TransactionServie'
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
        inboxConfig.ibobLocation = IBOB.location
    }

    main 
    {
        [react( req )()
        {   
            println@Console("Recieved message for operation react!")()
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

            with ( updateServiceCRequest ){
                .username = req.username
            }
            with ( outboxQuery ){
                    .txHandle = req.txHandle;
                    .topic = config.kafkaOutboxOptions.topic;
                    .operation = "react"
            }
            getJsonString@JsonUtils( updateServiceCRequest )( outboxQuery.data )

            updateOutbox@IBOB( outboxQuery )( updateResponse )
        }]
    }
}