from database import Database, ConnectionInfo
from console import Console
from time import Time
from json_utils import JsonUtils

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
    OneWay: startReadingMessages ( ForwarderServiceInfo )
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
    embed Database as Database
    embed Time as Time
    embed Console as Console
    embed JsonUtils as JsonUtils
    
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
        [startReadingMessages( request )] 
        {
            // Keep polling for messages at a given interval.
            while(true) {
                connect@Database( connectionInfo )( void )
                query = "SELECT * FROM outbox LIMIT " + request.pollSettings.pollAmount
                
                query@Database(query)( pulledMessages )
                println@Console( "Query '" + query + "' returned " + #pulledMessages.row + " rows " )(  )
                
                if (#pulledMessages.row > 0){
                    for ( databaseMessage in pulledMessages.row ){
                        kafkaMessage.topic = "example"
                        kafkaMessage.key = databaseMessage.(request.columnSettings.keyColumn)

                        // Create a Json message to enter into Kafka, for easy parsing on consumer end
                        getJsonString@JsonUtils({
                            parameters = databaseMessage.(request.columnSettings.valueColumn), 
                            mid = databaseMessage.mid
                        }
                        )(kafkaMessage.value)

                        kafkaMessage.brokerOptions << request.brokerOptions
                        propagateMessage@KafkaRelayer( kafkaMessage )( kafkaResponse )

                        if (kafkaResponse.status == 200) {
                            update@Database( "DELETE FROM outbox WHERE mid = \"" + databaseMessage.mid + "\";" )( updateResponse )
                        }
                    }
                }
                sleep@Time( request.pollSettings.pollDurationMS )(  )
            }
        }
    }
}