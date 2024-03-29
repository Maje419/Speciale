from console import Console
from time import Time

// Communicate back to the inboxWriter which writes messages to the inbox
from .inboxWriter import InboxWriterKafkaInterface

// The type of the parameter is located along the other inbox types
from .inboxTypes import MRSEmbeddingConfig

// We use a Java class which uses the kafka library to connect to Kafka
from .kafka-retriever import KafkaConsumer

/** This interface contains the operation which is the main loop of the MRS */
interface MessageRetrieverInterface{
    OneWay:
        /** This operation polls the Java Kafka Service for new messages, then calls the InboxWriter to insert these in the inbox table. */
        beginReading
}

service MessageRetriever(p: MRSEmbeddingConfig) {
    execution: concurrent

    /** This port is used to notify the inbox service of new messages */
    outputPort InboxWriter {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: InboxWriterKafkaInterface            
    }

    /** This port is used by the MRS to call itself when it finishes one iteration of its main loop */
    inputPort Self {
        Location: "local"
        Interfaces: 
            MessageRetrieverInterface
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
            // Try reading some messages
            consume@KafkaConsumer( {timeoutMs = 3000} )( consumeResponse )
            println@Console( "MRS: Received " + #consumeResponse.messages + " messages from KafkaConsumerService" )(  )

            // For each message, write it to inbox, then commit.
            // Message does not count as consumed until committed, and is unique in the inbox, thus guarenteeing
            // exactly-once-delivery
            for ( i = 0, i < #consumeResponse.messages, i++ ) 
            {
                println@Console( "InboxService: Retrieved message: " + consumeResponse.messages[i].value + " at offset " + consumeResponse.messages[i].offset)( )
                recievedKafkaMessage << consumeResponse.messages[i]
                recieveKafka@InboxWriter( recievedKafkaMessage )( recievedKafkaMessageResponse )
                
                println@Console("MRS: InboxWriter finished")()

                commitRequest.offset = consumeResponse.messages[i].offset
                commit@KafkaConsumer( commitRequest )( commitResponse )
                println@Console("MRS: finished Comitting")()
            }

            scheduleTimeout@Time( 1000{
                    operation = "beginReading"
            } )(  )
        }
    }
}