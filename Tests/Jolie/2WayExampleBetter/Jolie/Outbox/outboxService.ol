
from .outboxTypes import OutboxSettings, UpdateOutboxRequest, StatusResponse
from .messageForwarderService import MessageForwarderInterface
from ..TransactionService.transactionService import TransactionServiceOperations
from ...test.testTypes import TestInterface

from runtime import Runtime
from console import Console
from database import Database
from string_utils import StringUtils
from time import Time

interface OutboxInterface{
    RequestResponse:
        updateOutbox( UpdateOutboxRequest )( StatusResponse ) throws TestException
}

/**
* This service is used to implement the outbox pattern. Given an SQL query and some message, it will atomically execute the query, as well write the message to a messages table.
* It will then embeds a 'RelayService', which reads from the 'Messages' table and forwards messages into Kafka.
*/
service Outbox(p: OutboxSettings){
    execution: concurrent
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface, TestInterface
    }

    outputPort TransactionService {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: TransactionServiceOperations
    }

    outputPort MFS {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: MessageForwarderInterface, TestInterface
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

        println@Console("OutboxService: \tInitializing connection to Kafka")();
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
            if (global.testParams.throw_on_updateOutbox_called){
                throw ( TestException, "throw_on_updateOutbox_called" )
            }

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
            if ( req.commitTransaction ){
                commit@TransactionService( req.tHandle )()
                res.message = "Transaction executed sucessfully"
                res.success = true 
            } else {
                res.message = "Update could not be executed"
                res.success = false
            }

            if (global.testParams.throw_after_transaction_committed){
                throw ( TestException, "throw_after_transaction_committed" )
            }

        }]

        [setupTest(req)(res){
            global.testParams << req.outboxService
            setupTest@MFS(req)(res)
        }]
    }
}