
from .outboxTypes import OutboxConfig, UpdateOutboxRequest, StatusResponse
from .messageForwarderService import MessageForwarderInterface
from ..TransactionService.transactionService import TransactionServiceOperations

from runtime import Runtime
from console import Console
from database import Database
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
    outputPort TransactionService {
        Location: "local"   // Overwritten in init
        Interfaces: TransactionServiceOperations
    }

    embed Runtime as Runtime
    embed Time as Time
    embed Console as Console
    embed Database as Database

    init {
        // Insert location of the transaction service embedded in main service
        TransactionService.location << p.transactionServiceLocation

        // Load MFS
        with (MFSParams){
            .databaseConnectionInfo << p.databaseConnectionInfo;
            .pollSettings << p.pollSettings;
            .columnSettings.keyColumn = "kafkaKey";
            .columnSettings.valueColumn = "kafkaValue";
            .columnSettings.idColumn = "mid";
            .brokerOptions << p.brokerOptions
        }
        loadEmbeddedService@Runtime({
            filepath = "messageForwarderService.ol"
            params << {
                .databaseConnectionInfo << p.databaseConnectionInfo;
                .pollSettings << p.pollSettings;
                .columnSettings.keyColumn = "kafkaKey";
                .columnSettings.valueColumn = "kafkaValue";
                .columnSettings.idColumn = "mid";
                .brokerOptions << p.brokerOptions
            }
        })( MFS.location )

        // Connect to the database
        connect@Database( p.databaseConnectionInfo )( void )
        scope ( createMessagesTable )
        {
            install ( SQLException => { println@Console("Error when creating the outbox table for the outbox!")() })

            // Varchar size is not enforced by sqlite, we can insert a string of any length
            createTableRequest = "CREATE TABLE IF NOT EXISTS outbox (kafkaKey TEXT, kafkaValue TEXT, mid TEXT UNIQUE);"
            update@Database( createTableRequest )( ret )
        }
    }
    
    main {
        [updateOutbox( req )( res ){
            install (ConnectionError => {
                println@Console("Call to update before connecting")();
                res.message = "Call to update before connecting!";
                res.success = false
            })

            // We assume that the proccessID is unique to this process, and the same process cannot handle two requests concurrently.
            // This means the combination of currentTime and processId is unique to this message
            getProcessId@Runtime( )( processId )
            getCurrentTimeMillis@Time()( timeMillis )
            messageId = processId + ":" + timeMillis

            updateMessagesTableQuery = "INSERT INTO outbox (kafkaKey, kafkaValue, mid) VALUES ('" + req.operation + "', '" + req.data + "', '" + messageId + "');" 
            
            println@Console("Query: " + updateMessagesTableQuery)()
            with ( updateRequest ){
                .handle = req.tHandle;
                .update = updateMessagesTableQuery
            }

            executeUpdate@TransactionService( updateRequest )( updateResponse )
            
            res.message = "Outbox updated"
            res.success = true
        }]
    }
}