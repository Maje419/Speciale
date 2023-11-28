include "database.iol"
include "console.iol"

from .testA import ServiceAInterface
from runtime import Runtime

type UR {
    .name: string
    .number: int
}

interface ServiceBInterface {
    RequestResponse: update( UR )( string )
}

type ServiceBProperties{
    .serviceALocation: any
}

service ServiceB(p: ServiceBProperties){
    execution: concurrent
    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    outputPort ServiceA {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }
    embed Runtime as Runtime

    init {
        ServiceA.location = p.serviceALocation
    }

    main {
        [update( req ) ( res ){
            println@Console( "Hello from Service B with name: " + req.name + " and number: " + req.number )(  )
            res = "Nice"
        }]
    }
}