from console import Console
from time import Time

// Communicate back to the inboxWriter which writes messages to the inbox
from .inboxWriter import InboxWriterKafkaInterface

// The type of the parameter is located along the other inbox types
from .inboxTypes import MRSEmbeddingConfig

// We use a Java class which uses the kafka library to connect to Kafka
from .kafka-retriever import KafkaConsumer

service MessageRetriever(p: MRSEmbeddingConfig) {
    // This port is used to notify the inbox service of new messages
    outputPort InboxService {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: InboxWriterKafkaInterface            
    }
    embed KafkaConsumer as KafkaConsumer
    embed Time as Time
    embed Console as Console 

    init
    {
        // Overwrite so we can contact the Inbox Service
        InboxService.location << p.inboxServiceLocation

        // Initialize the Kafka Consumer
        with ( inboxSettings ){
            .pollOptions << p.kafkaPollOptions;
            .brokerOptions << p.kafkaInboxOptions
        }

        initialize@KafkaConsumer( inboxSettings )( initializedResponse )
        println@Console( "MessageRetriever Initialized" )(  )
    }

    main
    {
        while (true) 
        {
            consume@KafkaConsumer( {timeoutMs = 3000} )( consumeResponse )
            println@Console( "InboxService: Received " + #consumeResponse.messages + " messages from KafkaConsumerService" )(  )

            for ( i = 0, i < #consumeResponse.messages, i++ ) 
            {
                println@Console( "InboxService: Retrieved message: " + consumeResponse.messages[i].value + " at offset " + consumeResponse.messages[i].offset)( )
                recievedKafkaMessage << consumeResponse.messages[i]
                recieveKafka@InboxService( recievedKafkaMessage )( recievedKafkaMessageResponse )
                if ( recievedKafkaMessageResponse == "Message stored" ||
                     recievedKafkaMessageResponse == "Message already recieveid, please re-commit" )
                {
                    commitRequest.offset = consumeResponse.messages[i].offset
                    commit@KafkaConsumer( commitRequest )( commitResponse )
                }
            }
            sleep@Time( 1000 )(  )
        }
    }
}