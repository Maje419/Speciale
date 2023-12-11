from .serviceBInterface import ServiceBInterface
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
        })( _ )

        connect@Database(connectionInfo)()
        update@Database("CREATE TABLE IF NOT EXISTS NumbersB(username VARCHAR(50) NOT NULL, " +
                "number int)")()
    }

    main 
    {
        [updateLocalDb( message )]{
            println@Console("UpdateMessage Called")()
            update@Database("UPDATE NumbersB SET number = number + 1 WHERE username = \"user1\"")()
        }
    }
}