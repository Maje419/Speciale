from .inboxTypes import InboxEmbeddingConfig, InboxInterface

from console import Console
from time import Time
from .simple-kafka-connector import SimpleKafkaConsumerConnector

interface MessageRetrieverInterface{
    OneWay: beginReading( void )
}
service MessageRetrieverService(p: InboxEmbeddingConfig) {
    execution: concurrent
    outputPort InboxPort {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: InboxInterface            
    }
    inputPort Self {
        Location: "local"
        Interfaces: MessageRetrieverInterface
    }

    embed Console as Console
    embed Time as Time
    
    embed SimpleKafkaConsumerConnector as KafkaConsumerConnector

    init
    {
        InboxPort.location << p.localLocation

        with ( pollOptions )
        {
            .pollAmount = 3;
            .pollDurationMS = 3000
        };

        with ( kafkaOptions )
        {
            .bootstrapServer =  "localhost:29092";
            .groupId = "test-group";
            .topic = "example"
        };

        // Initialize Inbox Service
        with ( inboxSettings ){
            .pollOptions << pollOptions;
            .brokerOptions << kafkaOptions
        }

        Initialize@KafkaConsumerConnector( inboxSettings )( initializedResponse )

        scheduleTimeout@Time( 500{
                operation = "beginReading"
        } )(  )
    }

    main{
        [beginReading()]{
            // This might be bad code, but catch every exception and retry the call
            install (default => {
                scheduleTimeout@Time( 500{
                operation = "beginReading"
            } )(  )
            })
            consumeRequest.timeoutMs = 3000

            Consume@KafkaConsumerConnector( consumeRequest )( consumeResponse )
            println@Console( "InboxService: Received " + #consumeResponse.messages + " messages from KafkaConsumerService" )(  )

            for ( i = 0, i < #consumeResponse.messages, i++ ) {
                println@Console( "InboxService: Retrieved message: " + consumeResponse.messages[i].value + " at offset " + consumeResponse.messages[i].offset)( )
                recievedKafkaMessage << consumeResponse.messages[i]
                recieveKafka@InboxPort( recievedKafkaMessage )( recievedKafkaMessageResponse )
                if ( recievedKafkaMessageResponse == "Message stored" ||
                    recievedKafkaMessageResponse == "Message already recieveid, please re-commit" ){
                    commitRequest.offset = consumeResponse.messages[i].offset
                    Commit@KafkaConsumerConnector( commitRequest )( commitResponse )
                }
            }

            scheduleTimeout@Time( 500{
                operation = "beginReading"
            } )(  )
        }
    }
}