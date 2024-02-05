from .simple-kafka-connector import SimpleKafkaConsumerConnector
from ..test.testTypes import ProducerTests
from .inboxTypes import InboxEmbeddingConfig, InboxInterface

from console import Console
from time import Time

interface MessageRetrieverInterface{
    OneWay: 
        beginReading( void )
    RequestResponse: 
        setupTest( ProducerTests )( bool )
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
            // Restart operation if an exception occurs
            install (default => {
                scheduleTimeout@Time( 10{
                    operation = "beginReading"
            } )(  )
            })

            consumeRequest.timeoutMs = 3000

            Consume@KafkaConsumerConnector( consumeRequest )( consumeResponse )


            for ( i = 0, i < #consumeResponse.messages, i++ ) {
                
                if (global.testParams.throw_after_message_found && !global.hasThrownAfterForMessage){
                    global.hasThrownAfterForMessage = true
                    throw (TestException, "throw_after_message_found")
                }

                recievedKafkaMessage << consumeResponse.messages[i]

                /**
                The two options for the response from the Inbox are
                'The message was stored succesfully' or 
                'The message was already in the inbox. Please try commiting the offset to kafka again.'
                In both of these cases, the MRS should simply attempt to commit the offset to Kafka.

                In the case where the Inbox crashes, the MRS should react to this exception by breaking the for-loop, as
                to not commit some offset that is later than one it has not committed. This means that messages are processed
                sequentially.
                */
                recieveKafka@InboxPort( recievedKafkaMessage )( recievedKafkaMessageResponse )
                
                if (global.testParams.throw_after_notify_inbox_but_before_commit_to_kafka && !global.hasThrownAfterForMessage){
                    global.hasThrownAfterForMessage = true
                    throw (TestException, "throw_after_message_found")
                }

                if ( recievedKafkaMessageResponse == "Message stored" ||
                    recievedKafkaMessageResponse == "Message already recieveid, please re-commit" ){
                    commitRequest.offset = consumeResponse.messages[i].offset
                    Commit@KafkaConsumerConnector( commitRequest )( commitResponse )
                }
            }

            scheduleTimeout@Time( 10{
                operation = "beginReading"
            } )(  )
        }

        [setupTest( request )( response ){
            global.testParams << request.mrsTests
            global.hasThrownAfterForMessage = false
            response = true
        }]
    }
}