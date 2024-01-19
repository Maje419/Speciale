from console import Console
from time import Time

from ..test.testTypes import MRSTestParams
from .serviceBInterface import ServiceBInterface
from .simple-kafka-connector import SimpleKafkaConsumer

interface MRSInterface {
    OneWay: 
        pollKafka(void)
    RequestResponse:
        setupTest(MRSTestParams)(bool)
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

    // Used for sending messages to itself
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
        Initialize@KafkaConsumer("")
        scheduleTimeout@Time( 10{
                operation = "pollKafka"
        } )(  )
    }

    main 
    {
        [pollKafka()]{
            install ( TestException => {
                // Make sure that even when an exception is thrown for some message, the service does not stop recieving messages
                scheduleTimeout@Time( 500{
                operation = "pollKafka"
                } )(  )

                // Rethrow the same message
                println@Console("Exception thrown from mrs: " + main.TestException)()
                throw (TestException, main.TestException)
            } )
            
            Consume@KafkaConsumer("Consuming")( consumerMessage )
            if (#consumerMessage.messages > 0){

                for ( i = 0, i < #consumerMessage.messages, i++ ) {

                    // Throw only the first time, as if we always throw, even adding the inbox pattern would not help
                    if ( global.testParams.throw_on_message_found ){
                        global.testParams.throw_on_message_found = false
                        throw (TestException, "throw_on_message_found")
                    }

                    updateLocalDb@MainServicePort(consumerMessage.messages[i])

                    if (global.testParams.throw_after_updating_main_service){
                        global.testParams.throw_after_updating_main_service = false
                        throw (TestException, "throw_after_updating_main")
                    }
                }
            }
            if (global.testParams.throw_before_scheduling_timeout){
                global.testParams.throw_before_scheduling_timeout = false
                throw (TestException, "throw_before_timeout")
            }
            // This operation calls itself after .5 seconds
            scheduleTimeout@Time( 500 {
                operation = "pollKafka"
            } )(  )
        }

        [setupTest( req )( res ) {
            global.testParams << req
            res = true
        }]
    }
}