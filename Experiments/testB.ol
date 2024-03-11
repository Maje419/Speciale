include "database.iol"
include "console.iol"

from .testA import ServiceAInterface
from time import Time
from runtime import Runtime


interface ServiceBInterface {
    RequestResponse: update( string )( string )
    OneWay: updateState(int)
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
    embed Time as Time

    init {
        ServiceA.location = p.loc
    }

    main {
        [update( req ) ( res ){
            println@Console("Hello from B with state: " + global.p)()
            scheduleTimeout@Time( 1000{
                message = req
                operation = "update"
            } )( timer )
            res = "ok"
        }]

        [updateState(request)]{
            global.p = request
        }
    }
}