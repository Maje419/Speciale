from runtime import Runtime
from console import Console

from .publicInboxTypes import InboxConfig, InboxWriterExternalInterface
from .Inbox.inboxReader import InboxReaderInterface, InboxReaderService
from .Inbox.inboxWriter import InboxWriterService

from .publicOutboxTypes import OutboxInterface, OutboxConfig
from .Outbox.outboxService import OutboxService

interface IBOBInterface {
    RequestResponse: doNothing
}

type IBOBConfig{
    .inboxConfig[0,1]: InboxConfig
    .outboxConfig[0,1]: OutboxConfig
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
        if (is_defined(p.outboxConfig)){
            _setupOutboxDefaultValues

            // Load the Outbox service
            loadEmbeddedService@Runtime( {
                filepath = "Outbox/outboxService.ol"
                params << p.outboxConfig
            } )( Outbox.location )
        }
        
        if (is_defined(p.inboxConfig)){
            _setupInboxDefaultValues
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
    }

    main{
        doNothing()(){
            println@Console("Doing nothing")()
        }
    }

    define _setupInboxDefaultValues{
        if ((!is_defined(p.inboxConfig.pollTimer)) || p.pollTimer < 10){
            p.inboxConfig.pollTimer = 10
        }
        if ((!is_defined(p.inboxConfig.kafkaOptions.pollAmount)) || p.kafkaOptions.pollAmount < 1){
            p.inboxConfig.kafkaOptions.pollAmount = 5
        }
        if ((!is_defined(p.inboxConfig.kafkaOptions.pollTimeout)) || p.kafkaOptions.pollTimeout < 10){
            p.inboxConfig.kafkaOptions.pollTimeout = 10
        }
    }

    define _setupOutboxDefaultValues{
        if ((!is_defined(p.outboxConfig.pollTimer)) || p.outboxConfig.pollTimer < 10){
            p.outboxConfig.pollTimer = 10
        }
        if ((!is_defined(p.outboxConfig.pollAmount)) || p.outboxConfig.pollAmount < 1){
            p.outboxConfig.pollAmount = 3
        }
    }
}