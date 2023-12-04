include "database.iol"
include "console.iol"
include "serviceBInterface.iol"

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
            update@Database("UPDATE NumbersB SET number = number + 1 WHERE username = \"" + message + "\"")()
        }
    }
}