from .outboxTypes import ForwarderServiceInfo
from .kafka-inserter import KafkaInserter

from json-utils import JsonUtils
from time import Time
from database import Database
from console import Console

interface MessageForwarderInterface {
    OneWay: startReadingMessages ( ForwarderServiceInfo )
}

/**
* This service is responsible for reading messages from the 'outbox' table, forwarding them to Kafaka, 
* then deleting the messages from the table when an ack is recieved fom Kafka
*/
service MessageForwarderService{
    execution{ sequential }
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
    main{
        [startReadingMessages( request )] 
        {
            connect@Database( request.databaseConnectionInfo )( void )
            println@Console( "OutboxMessageForwarder Initialized" )(  )
            
            // Keep polling for messages at a given interval.
            while(true) {
                query = "SELECT * FROM outbox LIMIT " + request.pollSettings.pollAmount
                query@Database( query )( pulledMessages )
                if (#pulledMessages.row > 0){
                    println@Console( "OutboxMessageForwarder: \tForwarding " +  #pulledMessages.row + " messages into kafka!")(  )

                    for ( databaseMessage in pulledMessages.row ){
                        kafkaMessage.topic = request.brokerOptions.topic
                        kafkaMessage.key = databaseMessage.kafkakey
                        kafkaMessage.value = databaseMessage.kafkavalue
                        kafkaMessage.brokerOptions << request.brokerOptions

                        // We need the mid stored in kafka, since we need to ensure each message is only handled once
                        getJsonString@JsonUtils({
                            parameters = databaseMessage.kafkavalue, 
                            mid = databaseMessage.mid
                        }
                        )(kafkaMessage.value)

                        propagateMessage@KafkaInserter( kafkaMessage )( kafkaResponse )

                        println@Console( "Response status: " + kafkaResponse.success )(  )
                        if ( kafkaResponse.success ) {
                            deleteQuery = "DELETE FROM outbox WHERE mid = " + databaseMessage.mid
                            println@Console( "OutboxMessageForwarder: \tExecuting query '" + deleteQuery + "'")(  )
                            update@Database( deleteQuery )( updateResponse )
                        }
                    }
                }
                sleep@Time( request.pollSettings.pollDurationMS )(  )
            }
        }
    }
}