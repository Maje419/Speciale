from .outboxTypes import MFSParams, MessageForwarderInterface
from .kafka-inserter import KafkaInserter
from ...test.testTypes import TestInterface


from json-utils import JsonUtils
from time import Time
from database import Database
from console import Console

/**
* This service is responsible for reading messages from the 'outbox' table, forwarding them to Kafaka, 
* then deleting the messages from the table when an ack is recieved fom Kafka
*/
service MessageForwarderService(p: MFSParams){
    execution: concurrent

    inputPort Input {
        Location: "local"
        Interfaces: MessageForwarderInterface, TestInterface
    }

    embed KafkaInserter as KafkaInserter
    embed Time as Time
    embed Database as Database
    embed Console as Console
    embed JsonUtils as JsonUtils


    // Starts this service reading continually from the 'outbox' table
    init {
        connect@Database( p.databaseConnectionInfo )( void )
        println@Console( "OutboxMessageForwarder Initialized" )(  )

        scheduleTimeout@Time( 20{
                operation = "startReadingMessages"
        } )(  )
    }

    main{
        [startReadingMessages()] 
        {
            install (default => {
                println@Console("MFS: caught some error")()
                scheduleTimeout@Time( 20{
                    operation = "startReadingMessages"
                } )(  )
            })

            query = "SELECT * FROM outbox LIMIT " + p.pollSettings.pollAmount
            query@Database( query )( pulledMessages )
            if (#pulledMessages.row > 0){

                if (global.testParams.throw_after_message_found && !global.hasThrown){
                    global.hasThrown = true
                    throw (TestException, "throw_after_message_found")
                }

                println@Console( "OutboxMessageForwarder: \tForwarding " +  #pulledMessages.row + " messages into kafka!")(  )

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

                    println@Console("jsonstring: " + kafkaMessage.value)()

                    propagateMessage@KafkaInserter( kafkaMessage )( kafkaResponse )

                    if (global.testParams.throw_before_commit_to_kafka && !global.hasThrown){
                    global.hasThrown = true
                    throw (TestException, "throw_before_commit_to_kafka")
                }

                    println@Console( "Response status: " + kafkaResponse.success )(  )
                    if ( kafkaResponse.success ) {
                        deleteQuery = "DELETE FROM outbox WHERE mid = '" + databaseMessage.mid + "'"
                        println@Console( "OutboxMessageForwarder: \tExecuting query '" + deleteQuery + "'")(  )
                        update@Database( deleteQuery )( updateResponse )
                    }
                }
            }
            scheduleTimeout@Time( 20{
                operation = "startReadingMessages"
            } )(  )
        }

        [setupTest(req)(res){
            println@Console("Setuptests MFS")()

            global.testParams << req.MFS
            global.hasThrown = false
            res = true
        }]
    }
}