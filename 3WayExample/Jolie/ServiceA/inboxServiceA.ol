from .serviceAInterface import ServiceAInterfaceExternal
from ..InboxOutbox.publicInboxTypes import IbobLocation, InboxWriterExternalInterface

// Defining the service. InboxConfig must be defined in the Embedder service, and passed when embedding.
service InboxServiceA (p: IbobLocation){
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
        IBOB.location << p
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