from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

from .serviceCInterface import ServiceCInterface
from ..InboxOutbox.publicOutboxTypes import OutboxInterface

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
        with ( connectionInfo ){
            .username = "postgres"
            .password = "example"
            .database = "service-c-db"
            .driver = "postgresql"
            .host = ""
        }

        with ( kafkaInboxOptions ){
            .bootstrapServer = "localhost:29092"
            .groupId = "service-c-inbox"
            .topic = "b-out"
        }
        
        with ( kafkaOutboxOptions ){
                .topic = "c-out"
                .bootstrapServer = "localhost:29092"
        }

        connect@Database( connectionInfo )( )
        update@Database( "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)" )( ret )

        getLocalLocation@Runtime()( location )

        with( inboxConfig ){ 
            .locations << {
                .localLocation << location;
                .databaseServiceLocation << Database.location
            }
            .kafkaOptions << kafkaInboxOptions
        }

        with ( outboxConfig ){
            .brokerOptions << kafkaOutboxOptions;
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
                    .topic = "c-out";
                    .operation = "finalizeChoreography"
            }
            getJsonString@JsonUtils( finalizeServiceARequest )( outboxQuery.parameters )

            updateOutbox@IBOB( outboxQuery )( updateResponse )
            
            res = true
        }]
    }
}