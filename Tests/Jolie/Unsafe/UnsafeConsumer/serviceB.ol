from .serviceBInterface import ServiceBInterface
from .messageRecieverService import MRSInterface
from ..test.testTypes import TestExceptionType
from database import Database
from console import Console
from runtime import Runtime

service ServiceB{
    execution: concurrent
    // inputPort ServiceB {
    //     location: "socket://localhost:8082" 
    //     protocol: http{
    //         format = "json"
    //     }
    //     interfaces: ServiceBInterface
    // }
    
    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    outputPort MRS {
        Location: "local"
        Interfaces: MRSInterface
    }

    embed Runtime as Runtime
    embed Database as Database
    embed Console as Console

    init 
    {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = ""
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "./UnsafeConsumer/messageRecieverService.ol"
            params << {
                mainServiceLocation << localLocation
            }
        })( MRS.location )

        connect@Database(connectionInfo)()
        update@Database("CREATE TABLE IF NOT EXISTS NumbersB(username VARCHAR(50) NOT NULL, " +
                "number int)")()
    }

    main 
    {
        [setupTest( req )( res ){
            global.testParams << req.serviceB
            res = true
        }] 
        {
            setupTest@MRS(req.serviceB.mrs)()
        }

        [updateLocalDb( message )]{
            if (global.testParams.throw_before_updating_local){
                throw (TestException, "throw_before_updating")
            }
            update@Database("UPDATE NumbersB SET number = number + 1 WHERE username = \"user1\"")()
            if (global.testParams.throw_after_updating_local){
                throw (TestException, "throw_after_updating")
            }
        }
    }
}