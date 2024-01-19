include "database.iol"
include "console.iol"
include "time.iol"

from .outboxService import KafkaOptions
from .outboxService import PollSettings
from .outboxService import StatusResponse
from .simple-kafka-connector import SimpleKafkaConnector

type ColumnSettings {
    .keyColumn: string
    .valueColumn: string
    .idColumn: string
}

type ForwarderServiceInfo {
    .databaseConnectionInfo: ConnectionInfo     // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                 // The settings to use
    .columnSettings: ColumnSettings            // The names of the columns in the 'messages' table
    .brokerOptions: KafkaOptions
}

type ForwarderResponse{
    .status: int
    .reason: string
}

interface MessageForwarderInterface {
    RequestResponse: startReadingMessages ( ForwarderServiceInfo ) ( StatusResponse )
}

/**
* This service is responsible for reading messages from the 'Messages' table, forwarding them to Kafaka, 
* then deleting the messages from the table when an ack is recieved fom Kafka
*/
service MessageForwarderService{
    inputPort Input {
        Location: "local"
        Interfaces: MessageForwarderInterface
    }
    embed SimpleKafkaConnector as KafkaRelayer

    init {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }
    }

    /** Starts this service reading continually from the 'Messages' table */
    main{
        [startReadingMessages( request )( response ) 
        {
            scope (ConnectToDatabase){      // Everything in this scope is simply to check that a connection to the database CAN be opened.
                global.M_KafkaOptions << request.brokerOptions
                response.status = 200
                response.reason = "MessageForwarderService initialized sucessfully"
                println@Console( "MessageForwarder connected to database!" )(  )
            }

        }] 
        {
            // Keep polling for messages at a given interval.
            while(true) {
                connect@Database( connectionInfo )( void )      //I hate that i cannot keep the connection open from above, but likely scoping issue
                query = "SELECT * FROM outbox LIMIT " + request.pollSettings.pollAmount
                query@Database(query)( pulledMessages )
                println@Console( "Query '" + query + "' returned " + #pulledMessages.row + " rows " )(  )
                if (#pulledMessages.row > 0){
                    for ( databaseMessage in pulledMessages.row ){
                        kafkaMessage.topic = "example"
                        kafkaMessage.key = databaseMessage.(request.columnSettings.keyColumn)
                        kafkaMessage.value = databaseMessage.(request.columnSettings.valueColumn)
                        kafkaMessage.brokerOptions << global.M_KafkaOptions
                        propagateMessage@KafkaRelayer( kafkaMessage )( kafkaResponse )
                        if (kafkaResponse.status == 200) {
                            update@Database( "DELETE FROM outbox WHERE " + ( request.columnSettings.idColumn ) + " = " + databaseMessage.(request.columnSettings.idColumn) )( updateResponse )
                        }
                    }
                }
                sleep@Time( request.pollSettings.pollDurationMS )(  )
            }
        }
    }
}