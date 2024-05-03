from .outboxTypes import MFSParams
from .kafka-inserter import KafkaInserter

from json-utils import JsonUtils
from time import Time
from database import Database
from console import Console


interface MessageForwarderInterface {
    RequestResponse: startReadingMessages ( void )(void )
}

/**
* This service is responsible for reading messages from the 'outbox' table, forwarding them to Kafaka, 
* then deleting the messages from the table when an ack is recieved fom Kafka
*/
service MessageForwarderService(p: MFSParams){
    execution: concurrent

    inputPort Input {
        Location: "local"
        Interfaces: MessageForwarderInterface
    }

    embed KafkaInserter as KafkaInserter
    embed Time as Time
    embed Database as Database
    embed Console as Console
    embed JsonUtils as JsonUtils


    // Starts this service reading continually from the 'outbox' table
    init {
        connect@Database( p.databaseConnectionInfo )( void )

        scheduleTimeout@Time( 1000{
                operation = "startReadingMessages"
        } )(  )
    }

    main{
        [startReadingMessages()(){
            install (default => {
                scheduleTimeout@Time( 1000{
                    operation = "startReadingMessages"
                } )(  )
            })

            query = "SELECT * FROM outbox LIMIT " + p.pollSettings.pollAmount
            query@Database( query )( pulledMessages )

            if (#pulledMessages.row > 0){

                for ( databaseMessage in pulledMessages.row ){
                    kafkaMessage.topic = p.brokerOptions.topic
                    kafkaMessage.key = databaseMessage.kafkakey
                    kafkaMessage.value = databaseMessage.kafkavalue
                    kafkaMessage.brokerOptions << p.brokerOptions

                    // We need the mid stored in kafka, since we need to ensure each message is only handled once
                    getJsonString@JsonUtils({
                        parameters = databaseMessage.kafkavalue, 
                        mid = databaseMessage.mid
                    }
                    )(kafkaMessage.value)

                    propagateMessage@KafkaInserter( kafkaMessage )( kafkaResponse )

                    if ( kafkaResponse.success ) {
                        deleteQuery = "DELETE FROM outbox WHERE mid = '" + databaseMessage.mid + "'"
                        update@Database( deleteQuery )( updateResponse )
                    }
                }
            }
            scheduleTimeout@Time( 1000{
                operation = "startReadingMessages"
            } )(  )
        }] 
    }
}