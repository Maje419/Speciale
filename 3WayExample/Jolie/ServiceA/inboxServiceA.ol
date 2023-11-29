from runtime import Runtime
from console import Console

from ..Inbox.inboxTypes import InboxEmbeddingConfig
from ..Inbox.inboxWriter import InboxWriterService
from ..Inbox.inboxReader import InboxReaderService
from .serviceAInterface import ServiceAInterfaceExternal

service InboxServiceA (p: InboxEmbeddingConfig){
    execution: concurrent

    inputPort ServiceAExternal {
        Location: "socket://localhost:8080"
        Protocol: http{
            format = "json"
        }
        Interfaces: ServiceAInterfaceExternal
    }

    embed Console as Console
    embed Runtime as Runtime
    embed InboxWriterService(p) as InboxWriter
    embed InboxReaderService(p) as InboxReader
    
    main
    {
        [ updateNumber( req )( res )
        {
            with( inboxMessage )
            {
                .operation = "updateNumber";
                .request << req
            }

            insertIntoInbox@InboxWriter( inboxMessage )
            res = "Choreography Started!"
        }]
    }
}