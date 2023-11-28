include "database.iol"
include "console.iol"

from runtime import Runtime
from reflection import Reflection
from .testB import ServiceB
from .testB import ServiceBInterface

type NameRequest {
    .name: string
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
    embed Runtime as Runtime
    embed Reflection as Reflection

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
            with ( invokeRequest ){
                .outputPort = "ServiceB";
                .data << {.name = req.name; .number = 12};
                .operation = "update"
            }
            invoke@Reflection( invokeRequest )( resp )
            // update@ServiceB( req.name )( resp )
            res = "Updated"
        }] 

        [finishChoreography( req )( res ){
            println@Console( "Choreography has been finished with request: " + req )(  )
            res = "Nice"
        }]
    }
}