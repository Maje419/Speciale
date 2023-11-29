from ..Inbox.inboxTypes import InboxEmbeddingConfig, EmbedderServiceHandle
from ..Inbox.inboxWriter import InboxWriterService
from ..Inbox.inboxReader import InboxReaderService
from .serviceAInterface import ServiceAInterfaceExternal

// Defining the service. InboxEmbeddingConfig must be defined in the Embedder service, and passed when embedding.
service InboxServiceA (p: InboxEmbeddingConfig){
    execution: concurrent

    // This port is what ServiceA used to listen on. This InboxService now takes ownership of it. 
    inputPort ServiceAExternal {
        Location: "socket://localhost:8080"
        Protocol: http{
            format = "json"
        }
        Interfaces: ServiceAInterfaceExternal
    }

    // This service can be called to insert new requests into the inbox, to ensure their delivery
    embed InboxWriterService(p) as InboxWriter
    // This service will poll the inbox, forwarding new messages to Service A, the embedder
    embed InboxReaderService(p) as InboxReader
    
    main
    {   
        // This operation is the only one which can be called from outside local, which means
        // it needs to be defined here.
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