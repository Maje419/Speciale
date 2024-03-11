from console import Console
from time import Time

// Communicate back to the inboxWriter which writes messages to the inbox
from .inboxWriter import InboxWriterKafkaInterface

// The type of the parameter is located along the other inbox types
from .inboxTypes import MRSEmbeddingConfig

// We use a Java class which uses the kafka library to connect to Kafka
from .kafka-retriever import KafkaConsumer

from ...test.testTypes import TestInterface

interface MessageRetrieverInterface{
    OneWay: 
        beginReading( void )
}

service MessageRetriever(p: MRSEmbeddingConfig) {
    execution: concurrent
    
    // This port is used to notify the inbox service of new messages
    outputPort InboxWriter {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: InboxWriterKafkaInterface            
    }

    inputPort Self {
        Location: "local"
        Interfaces: 
            MessageRetrieverInterface,
            TestInterface
    }

    embed KafkaConsumer as KafkaConsumer
    embed Time as Time
    embed Console as Console 

    init
    {
        // Overwrite so we can contact the Inbox Service
        InboxWriter.location << p.inboxServiceLocation

        // Initialize the Kafka Consumer
        with ( inboxSettings ){
            .pollOptions << p.kafkaPollOptions;
            .brokerOptions << p.kafkaInboxOptions
        }

        initialize@KafkaConsumer( inboxSettings )( initializedResponse )
        println@Console( "MessageRetriever Initialized" )(  )

        scheduleTimeout@Time( 500{
            operation = "beginReading"
        } )(  )
    }

    main
    {
        [beginReading()]{
            // Restart reading if something goes wrong
            install (default => {
                scheduleTimeout@Time( 10{
                    operation = "beginReading"
            } )(  )
            })

            // Try reading some messages
            consume@KafkaConsumer( {timeoutMs = 3000} )( consumeResponse )
            println@Console( "MRS: Received " + #consumeResponse.messages + " messages from KafkaConsumerService" )(  )

            // For each message, write it to inbox, then commit.
            // Message does not count as consumed until committed, and is unique in the inbox, thus guarenteeing
            // exactly-once-delivery
            for ( i = 0, i < #consumeResponse.messages, i++ ) 
            {
                if (global.testParams.throw_after_message_found && !global.hasThrownAfterForMessage){
                    println@Console("MRS Threw exception: " + "throw_after_message_found")()
                    global.hasThrownAfterForMessage = true
                    throw (TestException, "throw_after_message_found")
                }
                

                println@Console( "MRS: Retrieved message: " + consumeResponse.messages[i].value + " at offset " + consumeResponse.messages[i].offset)( )
                recievedKafkaMessage << consumeResponse.messages[i]
                recieveKafka@InboxWriter( recievedKafkaMessage )( recievedKafkaMessageResponse )

                if (global.testParams.throw_after_notify_inbox_but_before_commit_to_kafka && !global.hasThrownAfterForMessage){
                    global.hasThrownAfterForMessage = true
                    throw (TestException, "throw_after_message_found")
                }

                if ( recievedKafkaMessageResponse == "Message stored" ||
                    recievedKafkaMessageResponse == "Message already recieved, please re-commit" )
                {
                    commitRequest.offset = consumeResponse.messages[i].offset
                    commit@KafkaConsumer( commitRequest )( commitResponse )
                }
            }

            scheduleTimeout@Time( 10{
                    operation = "beginReading"
            } )(  )
        }

        [setupTest( request )( response ){
            println@Console("MRS tests")()

            global.testParams << request.MRS
            global.hasThrownAfterForMessage = false
            response = true
        }]
    }
}