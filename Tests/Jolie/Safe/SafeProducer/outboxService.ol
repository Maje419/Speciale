include "time.iol"
include "console.iol"
include "database.iol"
from .messageForwarderService import MessageForwarderService

from ..test.producerTestTypes import TestParams, TestExceptionType

type KafkaOptions: void {   
    .bootstrapServers: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
}

type RabbitMqOptions {      // Not implemented
    .bootstrapServers: string
    .groupId: string
}

type PollSettings: void{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type UpdateOutboxRequest{
    .sqlQuery: string                                   // The query that is to be executed against the database
    .key: string                                        // The key to use in the kafka message
    .value: string                                      // The value for the kafka message
    .topic: string                                      // The kafka topic on which the update should be broadcast
}

type ConnectOutboxRequest{
    .databaseConnectionInfo: ConnectionInfo             // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                         // Object of type PollSettings descibing the desired behaviour of the MessageForwarder
    .brokerOptions: KafkaOptions // RabbitMqOptions
}

type StatusResponse {
    .status: int
    .reason: string
}

interface OutboxInterface{
    RequestResponse:
        connectKafka( ConnectOutboxRequest ) ( StatusResponse ),
        connectRabbitMq( ConnectOutboxRequest ) ( StatusResponse ),
        transactionalOutboxUpdate( UpdateOutboxRequest )( StatusResponse ),
        setupTest( TestParams )( bool )
}

/**
* This service is used to implement the outbox pattern. Given an SQL query and some message, it will atomically execute the query, as well write the message to a messages table.
* It will then embeds a 'MessageForwarderService', which reads from the 'Messages' table and forwards messages into Kafka.
*/
service Outbox{
    execution: sequential
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface
    }
    embed MessageForwarderService as MessageForwarderService

    main {
        [setupTest( request )( response ){
            global.testParams << request.testParams.outboxtests
            setupTest@MessageForwarderService(request.testParams.mfsTests)( response )
        }]

        [connectKafka( request ) ( response ){
            connect@Database( request.databaseConnectionInfo )( void )
            scope ( createMessagesTable )
            {
                install ( SQLException => {
                    println@Console("Error when creating the messages table for the outbox!")();
                    response.reason = "Error when creating the messages table for the outbox!";
                    resposnse.code = 500
                    })

                // Varchar size is not enforced by sqlite, we can insert a string of any length
                updateRequest = "CREATE TABLE IF NOT EXISTS outbox (kafkaKey VARCHAR(50), kafkaValue VARCHAR (150), mid INTEGER PRIMARY KEY AUTOINCREMENT);"
                update@Database( updateRequest )( ret )
            }

            relayRequest.databaseConnectionInfo << request.databaseConnectionInfo
            relayRequest.pollSettings << request.pollSettings

            relayRequest.columnSettings.keyColumn = "kafkaKey"
            relayRequest.columnSettings.valueColumn = "kafkaValue"
            relayRequest.columnSettings.idColumn = "mid"
            relayRequest.brokerOptions << request.brokerOptions
            
            startReadingMessages@MessageForwarderService( relayRequest )
            response << {status = 200, reson = "Connected successfully to Kafka"}
        }]

        [transactionalOutboxUpdate( request )( response ){
            println@Console( "Initiating transactional update" )(  )
            install (ConnectionError => {response = "Call to update before connecting"} )

            updateMessagesTableQuery = "INSERT INTO outbox (kafkaKey, kafkaValue) VALUES (\"" + request.key + "\", \"" + request.value + "\");" 
            transactionRequest.statement[0] = updateMessagesTableQuery
            transactionRequest.statement[1] = request.sqlQuery

            if (global.testParams.throw_before_insert){
                throw ( TestException, "throw_before_insert" )
            }

            executeTransaction@Database( transactionRequest )( transactionResponse )

            if (global.testParams.throw_after_insert){
                throw ( TestException, "throw_after_insert" )
            }
            
            response.status = 200
            response.reason = "Transaction executed sucessfully"
        }]
    }
}