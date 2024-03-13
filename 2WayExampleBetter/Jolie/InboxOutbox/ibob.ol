from runtime import Runtime
from console import Console

from ..Inbox.inboxTypes import InboxConfig
from ..Inbox.inboxReader import InboxReaderInterface, InboxReaderService
from ..Inbox.inboxWriter import InboxWriterExternalInterface, InboxWriterService

from ..Outbox.outboxService import OutboxInterface, OutboxService
from ..Outbox.outboxTypes import OutboxConfig

interface IBOBInterface {
    RequestResponse: doNothing
}

type IBOBConfig{
    .inboxConfig: InboxConfig
    .outboxConfig: OutboxConfig
}

service IBOB(p: IBOBConfig){
    execution: concurrent

    outputPort Outbox {
        Location: "local"   // overwritten in init
        Interfaces: OutboxInterface
    }

    outputPort InboxWriter {
        Location: "local"  // overwritten in init
        Interfaces: InboxWriterExternalInterface
    }

    inputPort IBOB {
        Location: "local"
        Interfaces: IBOBInterface
        Aggregates: Outbox, InboxWriter
    }

    embed Console as Console
    embed Runtime as Runtime
    
    init{
        // Load the Outbox service
        loadEmbeddedService@Runtime( {
            filepath = "Outbox/outboxService.ol"
            params << p.outboxConfig
        } )( Outbox.location )    // It is very important that this is a lower-case 'location', otherwise it doesn't work
                                        // Guess how long it took me to figure that out :)
        
        // Load the Inbox Writer, which is callable from any local service wanting to insert something into the inbox
        loadEmbeddedService@Runtime( {
            filepath = "Inbox/inboxWriter.ol"
            params << p.inboxConfig
        } )( InboxWriter.location )

        // Load the Inbox Reader, which is a poll-consumer from the inbox table. 
        loadEmbeddedService@Runtime( {
            filepath = "Inbox/inboxReader.ol"
            params << p.inboxConfig
        } )()
    }

    main{
        doNothing()(){
            println@Console("Doing nothing")()
        }
    }
    
}