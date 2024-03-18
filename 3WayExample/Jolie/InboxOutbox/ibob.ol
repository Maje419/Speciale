from runtime import Runtime
from console import Console

from ..Inbox.inboxTypes import InboxConfig
from ..Inbox.inboxReader import InboxReaderInterface, InboxReaderService
from ..Inbox.inboxWriter import InboxWriterExternalInterface, InboxWriterService

from ..Outbox.outboxService import OutboxInterface, OutboxService
from ..Outbox.outboxTypes import OutboxConfig

/** This interface is here to make this service not say 'Main procedure for service IBOB is not defined' lol. */
interface IBOBInterface {
    RequestResponse: doNothing
}

/** This type contains the two configurations that are needed for the inbox and the outbox to work */
type IBOBConfig{
    .inboxConfig: InboxConfig           //< The configuration for the inbox service
    .outboxConfig: OutboxConfig         //< The configuration for the outbox service
}


/**
    This service functions as a wrapper contining the needed functionality for both the inbox and outbox services.
    It works by aggregating the Outbox and InboxWriter operations, meaning it provides both, and then forwards whatever request it gets to the correct
    outputport

*/
service IBOB(p: IBOBConfig){
    execution: concurrent

    /** This port is used to send messages to the embedded outbox service */
    outputPort Outbox {
        Location: "local"   // overwritten in init
        Interfaces: OutboxInterface
    }

    /** This port is used to send messages to the embedded InboxWriter service, which will insert messages into the inbox */
    outputPort InboxWriter {
        Location: "local"  // overwritten in init
        Interfaces: InboxWriterExternalInterface
    }

    /** This port is used when some embedder service wants to send messages to either the InboxWriter or the Outbox service. */
    inputPort IBOB {
        Location: "local"
        Interfaces: IBOBInterface   // This should not be needed pleeeeeease :(
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
        // This stupid thing is here to make Jolie stop complaining that no main service is found. This error is a result of my wanting to use aggragate services.
        doNothing()(){
            println@Console("Doing nothing")()
        }
    }
    
}