from .serviceBInterface import ServiceBInterface
from .kafka-retriever-unsafe import KafkaConsumer, KafkaInboxOptions

from json-utils import JsonUtils
from console import Console

type MrsConfig{
    .kafkaOptions: KafkaInboxOptions
    .mainServiceLocation: any
}

service MRS(p: MrsConfig){
    outputPort ServiceB {
        location: "local"
        interfaces: ServiceBInterface
    }

    embed Console as Console
    embed KafkaConsumer as KafkaConsumer
    embed JsonUtils as JsonUtils

    init
    {
        ServiceB.location << p.mainServiceLocation
        initialize@KafkaConsumer(p.kafkaOptions)()
    }

    main 
    {
        while (true) {
            consume@KafkaConsumer()( consumerMessage )
            for ( i = 0, i < #consumerMessage.messages, i++ ) {
                getJsonValue@JsonUtils(consumerMessage.messages[i].value)(kafkaValue)
                //println@Console(consumerMessage.messages[i].value)()
                getJsonValue@JsonUtils(kafkaValue.parameters)(pars)
                //println@Console(pars.username)()
                react@ServiceB(pars)()
            }
        }
    }
}