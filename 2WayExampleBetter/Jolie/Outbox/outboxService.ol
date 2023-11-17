include "database.iol"
include "time.iol"
include "console.iol"
include "Outbox/outboxTypes.iol"

from .messageForwarderService import MessageForwarderService
from ..TransactionService.transactionService import TransactionServiceInterface

type OutboxSettings{
    .databaseConnectionInfo: ConnectionInfo             // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                         // Object of type PollSettings descibing the desired behaviour of the MessageForwarder
    .brokerOptions: KafkaOptions // RabbitMqOptions
    .transactionServiceLocation: any                    // The location of the transaction service
}

interface OutboxInterface{
    RequestResponse:
        updateOutbox( UpdateOutboxRequest )( StatusResponse )
    OneWay:
        connectKafka( OutboxSettings )
}

/**
* This service is used to implement the outbox pattern. Given an SQL query and some message, it will atomically execute the query, as well write the message to a messages table.
* It will then embeds a 'RelayService', which reads from the 'Messages' table and forwards messages into Kafka.
*/
service Outbox{
    execution: sequential
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface
    }

    outputPort TransactionService {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: TransactionServiceInterface
    }
    embed MessageForwarderService as MFS
    
    main {
        /*
        * Connect to a Kafka version of this service.
        */
        [connectKafka( request ) ]{
            TransactionService.location = request.transactionServiceLocation;
            println@Console("OutboxService: \tInitializing connection to Kafka")();
            connect@TransactionService( request.databaseConnectionInfo )( void )
            connect@Database( request.databaseConnectionInfo )( void )
            scope ( createMessagesTable )
            {
                install ( SQLException => { println@Console("Error when creating the outbox table for the outbox!")() })

                // Varchar size is not enforced by sqlite, we can insert a string of any length
                updateRequest = "CREATE TABLE IF NOT EXISTS outbox (kafkaKey VARCHAR(50), kafkaValue VARCHAR (150), mid SERIAL PRIMARY KEY);"
                update@Database( updateRequest )( ret )
            }

            relayRequest.databaseConnectionInfo << request.databaseConnectionInfo
            relayRequest.pollSettings << request.pollSettings

            relayRequest.columnSettings.keyColumn = "kafkaKey"
            relayRequest.columnSettings.valueColumn = "kafkaValue"
            relayRequest.columnSettings.idColumn = "mid"
            relayRequest.brokerOptions << request.brokerOptions
            
            startReadingMessages@MFS( relayRequest )
        }

        /*
        *Executes the request.query as well as writing the request.message to the messages table
        */
        [updateOutbox( req )( res ){
            updateOutboxTable = "INSERT INTO outbox (kafkaKey, kafkaValue) VALUES ('" + req.key + "', '" + req.value + "');" 

            with ( updateRequest ){
                .handle = req.tHandle;
                .update = updateOutboxTable
            }

            executeUpdate@TransactionService( updateRequest )( updateResponse )
            if ( req.commitTransaction ){
                commit@TransactionService( req.tHandle )()
            }
            res.success = true 
            res.message = "Transaction executed sucessfully"
        }]
    }
}