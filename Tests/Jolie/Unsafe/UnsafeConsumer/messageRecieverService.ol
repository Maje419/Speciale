include "console.iol"
include "time.iol"
include "serviceBInterface.iol"

from .simple-kafka-connector import SimpleKafkaConsumer

type MRSInput{
    .mainServiceLocation: any
}

service MRS(p: MRSInput){
    outputPort MainServicePort {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }
    embed SimpleKafkaConsumer as KafkaConsumer

    init
    {
        MainServicePort.location << p.mainServiceLocation
    }

    main 
    {
        Initialize@KafkaConsumer("Hi");
        while (true) {
            Consume@KafkaConsumer("Consuming")( consumerMessage )
            if (#consumerMessage.messages > 0){
                println@Console( consumerMessage.code )(  )
                for ( i = 0, i < #consumerMessage.messages, i++ ) {
                    updateLocalDb@MainServicePort(consumerMessage.messages[i])
                    println@Console("Message recieved from Kafka: " + consumerMessage.messages[i])()
                }
            }
        }
    }
}