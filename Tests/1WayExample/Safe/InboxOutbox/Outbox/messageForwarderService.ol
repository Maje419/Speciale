from ..publicOutboxTypes import OutboxConfig
from .kafka-inserter import KafkaInserter

from json-utils import JsonUtils
from time import Time
from database import DatabaseInterface
from console import Console

from ...test.testTypes import ProducerTestInterface

/** This interface contains only the message needed to start reading messages from the outbox table */
interface MessageForwarderInterface {
    RequestResponse: 
        /** This operation starts the MFS reading messages from the inbox service, then forwarding these into Kafka */
        startReadingMessages ( void )(void )
}

/**
* This service is responsible for reading messages from the 'outbox' table, forwarding them to Kafaka, 
* then deleting the messages from the table when an ack is recieved fom Kafka
*/
service MessageForwarderService(p: OutboxConfig){
    execution: concurrent

    /** This inputport is used when the MFS calls itself */
    inputPort Self {
        Location: "local"
        Interfaces: MessageForwarderInterface, ProducerTestInterface
    }

    /** This OutputPort is used when messaging the Database Service */
    outputPort Database {
        Location: "local" // Overwritten in init
        Interfaces: DatabaseInterface
    }

    embed KafkaInserter as KafkaInserter
    embed Time as Time
    embed Console as Console
    embed JsonUtils as JsonUtils


    // Starts this service reading continually from the 'outbox' table
    init {
        Database.location = p.databaseServiceLocation
        scheduleTimeout@Time( p.pollTimer{
                operation = "startReadingMessages"
        } )(  )
    }

    main{
        [startReadingMessages()(){
            install (TestException => {
                global.exceptionThrownForThisTest = true
                println@Console("Exception thrown from MFS: " + main.TestException)()

                //Call this operation again
                scheduleTimeout@Time( p.pollTimer{
                    operation = "startReadingMessages"
                } )(  )
            })
            
            // This throw should not cause desync issues
            if (global.testParams.throw_before_check_for_messages && !global.exceptionThrownForThisTest){
                throw ( TestException, "throw_before_check_for_messages" )
            }

            // Find any new messages in the outbox table
            query = "SELECT * FROM outbox LIMIT " + p.pollAmount
            query@Database( query )( pulledMessages )

            // This throw should not cause desync issues
            if (global.testParams.throw_after_message_found && !global.exceptionThrownForThisTest){
                throw ( TestException, "throw_after_message_found" )
            }

            if (#pulledMessages.row > 0){
                println@Console("startReadingMessages found a message!")()

                for ( databaseMessage in pulledMessages.row ){
                    
                    // Format the information contained in the found message in the form of a Kafka Message
                    kafkaMessage.topic = p.brokerOptions.topic
                    kafkaMessage.key = databaseMessage.operation
                    kafkaMessage.value = databaseMessage.parameters
                    kafkaMessage.brokerOptions << p.brokerOptions

                    // We need the mid stored in kafka, since we need to ensure each message is only handled once
                    getJsonString@JsonUtils({
                        parameters = databaseMessage.parameters, 
                        mid = databaseMessage.mid
                    }
                    )(kafkaMessage.value)

                    // Insert the message into kafka
                    propagateMessage@KafkaInserter( kafkaMessage )( kafkaResponse )

                    // This throw will likely result in the message being sent twice!
                    if (global.testParams.throw_after_send_but_before_delete && !global.exceptionThrownForThisTest){
                        throw ( TestException, "throw_after_send_but_before_delete" )
                    }

                    // If the message was successfully stored, we can delete it from the database
                    if ( kafkaResponse.success ) {
                        deleteQuery = "DELETE FROM outbox WHERE mid = '" + databaseMessage.mid + "'"
                        update@Database( deleteQuery )( updateResponse )
                    }
                    
                }
            }
            
            scheduleTimeout@Time( p.pollTimer{
                operation = "startReadingMessages"
            } )(  )
        }]

        [setupProducerTests( req )( res ){
            global.exceptionThrownForThisTest = false
            global.testParams << req.mfsTests
            res = true
        }]
    }
}