from .serviceAInterface import ServiceAInterfaceExternal
from ..Inbox.inboxTypes import InboxConfig
from ..Inbox.inboxWriter import InboxWriterExternalInterface

// Defining the service. InboxConfig must be defined in the Embedder service, and passed when embedding.
service InboxServiceA (p: InboxConfig){
    execution: concurrent

    // This port is what ServiceA used to listen on. This InboxService now takes ownership of it. 
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

    init {
        IBOB.location = p.ibobLocation
    }

    main
    {
        // This operation is the only one which can be called from outside local, which means
        // it needs to be defined here.
        [ startChoreography( req )( res )
        {
            with( inboxMessage )
            {
                .operation = "startChoreography";
                .request << req
            }
            
            insertIntoInbox@IBOB( inboxMessage )()
            res = "Choreography Started!"
        }]
    }
}