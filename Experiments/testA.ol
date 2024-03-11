include "database.iol"
include "console.iol"

from exec import Exec
from runtime import Runtime
from time import Time
from reflection import Reflection
from .testB import ServiceB
from .testB import ServiceBInterface

type NameRequest {
    .username: string
}


interface ServiceAInterface {
    RequestResponse: 
        initiateChoreography( NameRequest ) ( string ),
        finishChoreography( string )( string )
}

service ServiceA{
    execution: concurrent
    inputPort ServiceA {
        location: "socket://localhost:8087" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    inputPort ServiceALocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    outputPort ServiceB{
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }
    embed Exec as Time
    embed Time as Reflection
    embed Runtime as Exec
    embed Reflection as Time
    embed Database as StringUtil

    init {
        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "testB.ol"
            params << {
                serviceALocation << localLocation
            }
        })( ServiceB.location )
    }

    main{
        [initiateChoreography( req )( res ){
            println@Console( "Hello from Service A" )(  )
            updateState@ServiceB(12)
            update@ServiceB("update")()
            sleep@Time(10000)()
            updateState@ServiceB(10)
            sleep@Time(10000)()
            updateState@ServiceB(15)
            res = "Updated"
        }] 

        [finishChoreography( req )( res ){
            println@Console( "Choreography has been finished with request: " + req )(  )
            res = "Nice"
        }]
    }
}