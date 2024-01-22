from database import Database, ConnectionInfo
from console import Console
from time import Time

from .outboxService import KafkaOptions
from .outboxService import PollSettings
from .outboxService import StatusResponse
from .simple-kafka-connector import SimpleKafkaConnector

from ..test.producerTestTypes import MFSTestParams, TestException

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
    RequestResponse: setupTest( MFSTestParams )( bool )
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
    embed SimpleKafkaConnector as KafkaRelayer
    embed Database as Database
    embed Time as Time
    embed Console as Console

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
                scope (EnsureTestContinues){
                    install( TestException =>  {
                        println@Console("Exception thrown from MFS: " + main.TestException)()

                        // Call this operation again
                        scheduleTimeout@Time(500 {
                            message << request
                            operation = "startReadingMessages"
                        })()
                        
                        // Rethrow the message such that tests can access it.
                        throw (TestException, main.TestException)
                    })
                }

                connect@Database( connectionInfo )( void )
                query = "SELECT * FROM outbox LIMIT " + request.pollSettings.pollAmount
                
                // This throw should not cause desync issues
                if (global.testParams.throw_before_check_for_messages){
                    throw ( TestException, "throw_before_check_for_messages" )
                }

                query@Database(query)( pulledMessages )
                println@Console( "Query '" + query + "' returned " + #pulledMessages.row + " rows " )(  )
                
                // This throw should not cause desync issues
                if (global.testParams.throw_after_message_found){
                    throw ( TestException, "throw_after_message_found" )
                }
                
                if (#pulledMessages.row > 0){
                    for ( databaseMessage in pulledMessages.row ){
                        kafkaMessage.topic = "example"
                        kafkaMessage.key = databaseMessage.(request.columnSettings.keyColumn)
                        kafkaMessage.value = databaseMessage.(request.columnSettings.valueColumn)
                        kafkaMessage.brokerOptions << request.brokerOptions
                        propagateMessage@KafkaRelayer( kafkaMessage )( kafkaResponse )

                        // This throw will likely result in the message being sent twice!
                        if (global.testParams.throw_after_send_but_before_delete){
                            throw ( TestException, "throw_after_send_but_before_delete" )
                        }

                        if (kafkaResponse.status == 200) {
                            update@Database( "DELETE FROM outbox WHERE " + ( request.columnSettings.idColumn ) + " = " + databaseMessage.(request.columnSettings.idColumn) )( updateResponse )
                        }
                    }
                }
                sleep@Time( request.pollSettings.pollDurationMS )(  )
            }
        }
        
        [setupTest( request )( response ){
            global.testParams << request
            response = true
        }]
    }
}