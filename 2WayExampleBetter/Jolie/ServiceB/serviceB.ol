from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

from .serviceBInterface import ServiceBInterface
from ..InboxOutbox.publicOutboxTypes import OutboxInterface

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
        with ( connectionInfo ){
            .username = "postgres"
            .password = "example"
            .database = "service-b-db"
            .driver = "postgresql"
            .host = ""
        }

        with ( kafkaInboxOptions ){
            .bootstrapServer = "localhost:29092"
            .groupId = "service-b-inbox"
            .topic = "a-out"
            .pollAmount = 3
        }
        
        with ( kafkaOutboxOptions ){
                .topic = "b-out"
                .bootstrapServer = "localhost:29092"
        }

        connect@Database( connectionInfo )( )
        update@Database( "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)" )( ret )

        getLocalLocation@Runtime()( location )

        with( inboxConfig )
        { 
            .pollTimer = 10;
            .locations << {
                .localLocation << location;
                .databaseServiceLocation << Database.location
            }
            .kafkaOptions << kafkaInboxOptions
        }

        with ( outboxConfig ){
            .pollTimer = 10;
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
        [react( req )( res )
        {   
            println@Console("Recieved message for operation react!")()
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
                        .topic = "b-out";
                        .operation = "finalizeChoreography"
                }
                
                getJsonString@JsonUtils( updateServiceCRequest )( outboxQuery.parameters )

                updateOutbox@IBOB( outboxQuery )( updateResponse )
            }
            res = "Service B has updated locally"
        }]
    }
}