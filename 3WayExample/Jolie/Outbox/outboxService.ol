
from .outboxTypes import OutboxConfig, UpdateOutboxRequest, StatusResponse
from .messageForwarderService import MessageForwarderInterface

from runtime import Runtime
from console import Console
from database import DatabaseInterface
from string_utils import StringUtils
from time import Time

/** This interface provides the operation needed to insert a message into the outbox */
interface OutboxInterface{
    RequestResponse:
        /** Inserts a message in the outbox */
        updateOutbox( UpdateOutboxRequest )( StatusResponse )
}

/**
    This service is used to implement the outbox pattern. Given an SQL query and some message, it will atomically execute the query, as well write the message to a messages table.
    It will then embeds a 'RelayService', which reads from the 'Messages' table and forwards messages into Kafka.

    Outbox table:
*           _________________________________________________________
*           |       kafkaKey       |       kafkaValue      |   mid   |
*           |——————————————————————|———————————————————————|—————————|
*           |     "updateNumber"   |  {"username":"user1"} | 13:42.. |
*           |——————————————————————|———————————————————————|—————————|
*           |     "finalizeChor"   |  {"username":"user3"} | 13:37.. |
*           |——————————————————————|———————————————————————|—————————|
*/
service OutboxService(p: OutboxConfig){
    execution: concurrent

    /** This port is used when some service wants to write some message to the outbox */
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface
    }

    /** This port is used to talk to the transaction service */
    outputPort Database {
        Location: "local"   // Overwritten in init
        Interfaces: DatabaseInterface
    }

    embed Runtime as Runtime
    embed Time as Time
    embed Console as Console

    init {
        // Insert location of the transaction service embedded in main service
        Database.location << p.databaseServiceLocation

        // Load MFS
        loadEmbeddedService@Runtime({
            filepath = "messageForwarderService.ol"
            params << {
                .databaseConnectionInfo << p.databaseConnectionInfo;
                .databaseServiceLocation << p.databaseServiceLocation;
                .pollSettings << p.pollSettings;
                .columnSettings.keyColumn = "kafkaKey";
                .columnSettings.valueColumn = "kafkaValue";
                .columnSettings.idColumn = "mid";
                .brokerOptions << p.brokerOptions
            }
        })( MFS.location )

        // Connect to the database
        update@Database( "CREATE TABLE IF NOT EXISTS outbox (kafkaKey TEXT, kafkaValue TEXT, mid TEXT UNIQUE);" )( ret )
    }
    
    main {
        [updateOutbox( req )( res ){
            install (ConnectionError => {
                res.message = "Call to update before connecting!";
                res.success = false
            })

            // We assume that the proccessID is unique to this process, and the same process cannot handle two requests concurrently.
            // This means the combination of currentTime and processId is unique to this message
            getProcessId@Runtime( )( processId )
            getCurrentTimeMillis@Time()( timeMillis )
            messageId = processId + ":" + timeMillis

            updateMessagesTableQuery = "INSERT INTO outbox (kafkaKey, kafkaValue, mid) VALUES ('" + req.operation + "', '" + req.data + "', '" + messageId + "');" 
            
            with ( updateRequest ){
                .txHandle = req.txHandle;
                .update = updateMessagesTableQuery
            }

            update@Database( updateRequest )( updateResponse )
            
            println@Console("Outbox: Updated outbox with key: " + req.operation)()
            res.message = "Outbox updated"
            res.success = true
        }]
    }
}