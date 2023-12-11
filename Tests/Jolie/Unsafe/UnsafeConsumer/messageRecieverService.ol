from console import Console
from time import Time

from ..test.testTypes import TestParams, TestExceptionType
from .serviceBInterface import ServiceBInterface
from .simple-kafka-connector import SimpleKafkaConsumer

interface MRSInterface {
    OneWay: 
        pollKafka(void),
        setupTest(TestParams)
}
type MRSInput{
    .mainServiceLocation: any
    .testParams*: any{?}
}

service MRS(p: MRSInput){
    execution: concurrent
    outputPort MainServicePort {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    inputPort Self {
        location: "local"
        interfaces: MRSInterface
    }

    embed Time as Time
    embed Console as Console
    embed SimpleKafkaConsumer as KafkaConsumer

    init
    {
        MainServicePort.location << p.mainServiceLocation
        global.testParams << p.testParams
        Initialize@KafkaConsumer("")
        scheduleTimeout@Time( 10{
                operation = "pollKafka"
        } )(  )
    }

    main 
    {
        [pollKafka()]{
            println@Console("HERE!")()
            Consume@KafkaConsumer("Consuming")( consumerMessage )
            if (#consumerMessage.messages > 0){
                for ( i = 0, i < #consumerMessage.messages, i++ ) {
                    if (global.testParams.throw_in_MRS){
                        throw TestExceptionType(TestException)
                    }
                    updateLocalDb@MainServicePort(consumerMessage.messages[i])
                    println@Console("Message recieved from Kafka: " + consumerMessage.messages[i])()
                }
            }

            
            // This operation calls itself after 1 second
            scheduleTimeout@Time( 500{
                operation = "pollKafka"
            } )(  )
        }

        [setupTest( req )]{
            global.testParams << req
        }
    }
}