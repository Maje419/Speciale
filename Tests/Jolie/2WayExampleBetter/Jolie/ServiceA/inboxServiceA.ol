from runtime import Runtime
from console import Console

from .serviceAInterface import ServiceAInterfaceExternal, ServiceAInterfaceLocal
from ..Inbox.inboxTypes import InboxEmbeddingConfig
from ..Inbox.inboxWriter import InboxWriterService
from ..Inbox.inboxReader import InboxReaderService
from ...test.testTypes import TestInterface

service InboxServiceA (p: InboxEmbeddingConfig){
    execution: concurrent

    inputPort ServiceAExternal {
        Location: "socket://localhost:8080"
        Protocol: http{
            format = "json"
        }
        Interfaces: ServiceAInterfaceExternal
    }

    inputPort ServiceALocal {
        Location: "local"
        Interfaces: TestInterface
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

            if (global.testParams.throw_before_insert){
                throw (TestException, "throw_before_insert")
            }

            insertIntoInbox@InboxWriter( inboxMessage )( IWRes )

            if (global.testParams.throw_after_insert){
                throw (TestException, "throw_after_insert")
            }

            if (IWRes = "Message Stored") {
                res = "Choreography Started!"
            } else {
                res = "Message not received correctly"
            }
        }]
        
        [setupTest( req )( res ){
            println@Console("InboxServiceA tests")()
            global.testParams << req.inboxService
            setupTest@InboxWriter(req)(inboxWriterRes)
            setupTest@InboxReader(req)(inboxReaderRes)

            res = (inboxWriterRes && inboxReaderRes)
        }]
    }
}