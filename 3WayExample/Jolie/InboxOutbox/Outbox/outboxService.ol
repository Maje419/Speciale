
from ..publicOutboxTypes import UpdateOutboxRequest, OutboxConfig, StatusResponse
from .messageForwarderService import MessageForwarderInterface

from ..publicOutboxTypes import OutboxInterface

from runtime import Runtime
from console import Console
from database import DatabaseInterface
from string_utils import StringUtils
from time import Time

/**
* This service is used to implement the outbox pattern. Given an SQL query and some message, it will atomically execute the query, as well write the message to a messages table.
* It will then embeds a 'RelayService', which reads from the 'Messages' table and forwards messages into Kafka.
*           _________________________________________________________
*           |       operation      |       parameters      |   mid   |
*           |——————————————————————|———————————————————————|—————————|
*           |        "react"       |  {"username":"user1"} | 13:42.. |
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

    /** This port is used to talk to the Database Service */
    outputPort Database {
        Location: "local"   // Overwritten in init
        Interfaces: DatabaseInterface
    }

    embed Runtime as Runtime
    embed Time as Time
    embed Console as Console

    init {
        // Insert location of the Database service embedded in main service
        Database.location << p.databaseServiceLocation

        // Load MFS
        loadEmbeddedService@Runtime({
            filepath = "messageForwarderService.ol"
            params << p
        })( MFS.location )

        // Connect to the database
        update@Database( "CREATE TABLE IF NOT EXISTS outbox (operation TEXT, parameters TEXT, mid TEXT UNIQUE);" )( ret )
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

            updateMessagesTableQuery = "INSERT INTO outbox (operation, parameters, mid) VALUES ('" + req.operation + "', '" + req.parameters + "', '" + messageId + "');" 
            
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