from runtime import Runtime
from console import Console

from .serviceAInterface import ServiceAInterfaceExternal
from ..InboxOutbox.publicInboxTypes import IbobLocation, InboxWriterExternalInterface

service InboxServiceA (p: IbobLocation){
    execution: concurrent

    inputPort ServiceAExternal {
        Location: "socket://localhost:8080"
        Protocol: http{
            format = "json"
        }
        Interfaces: ServiceAInterfaceExternal
    }

    outputPort IBOB {
        Location: "local" // overwritten in init
        Interfaces: InboxWriterExternalInterface
    }
    
    embed Console as Console
    embed Runtime as Runtime
    
    init {
        IBOB.location = p
    }

    main
    {
        [ startChoreography( req )( res )
        {
            with( inboxMessage )
            {
                .operation = "startChoreography";
                .request << req
            }

            insertIntoInbox@IBOB( inboxMessage )( IWRes )

            if (IWRes == "Message stored") {
                res = "Choreography Started!"
            } else {
                res = "Message not received correctly"
            }
        }]
    }
}