from console import Console
from time import Time

from .inboxTypes import InboxInterface, MRSEmbeddingConfig
from .kafka-retriever import KafkaConsumer

service MessageRetriever(p: MRSEmbeddingConfig) {
    // This port is used to notify the inbox service of new messages
    outputPort InboxPort {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: InboxInterface            
    }
    embed KafkaConsumer as KafkaConsumer
    embed Time as Time
    embed Console as Console 

    init
    {
        // Overwrite so we can contact the Inbox Service
        InboxPort.location << p.localLocation

        println@Console(" poll: " + p.pollOptions.pollDurationMS )(  )

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
        
        consumeRequest.timeoutMs = 3000

        while (true) {
            consume@KafkaConsumer( consumeRequest )( consumeResponse )
            println@Console( "InboxService: Received " + #consumeResponse.messages + " messages from KafkaConsumerService" )(  )

            for ( i = 0, i < #consumeResponse.messages, i++ ) {
                println@Console( "InboxService: Retrieved message: " + consumeResponse.messages[i].value + " at offset " + consumeResponse.messages[i].offset)( )
                recievedKafkaMessage << consumeResponse.messages[i]
                recieveKafka@InboxPort( recievedKafkaMessage )( recievedKafkaMessageResponse )
                if ( recievedKafkaMessageResponse == "Message stored" ||
                     recievedKafkaMessageResponse == "Message already recieveid, please re-commit" ){
                    commitRequest.offset = consumeResponse.messages[i].offset
                    commit@KafkaConsumer( commitRequest )( commitResponse )
                }
            }
            sleep@Time( 1000 )(  )
        }
    }
}