from runtime import Runtime
from console import Console

from .serviceAInterface import ServiceAInterfaceExternal
from ..Inbox.inboxTypes import InboxConfig
from ..Inbox.inboxWriter import InboxWriterExternalInterface, InboxWriterService
from ..Inbox.inboxReader import InboxReaderService

service InboxServiceA (p: InboxConfig){
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
        IBOB.location = p.ibobLocation
    }

    main
    {
        [ updateNumber( req )( res )
        {
            with( inboxMessage )
            {
                .operation = "updateNumber";
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