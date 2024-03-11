from time import Time
from console import Console
from runtime import Runtime

interface ProccessIDInterface {
    OneWay: printId(void)
}

service processIdTest {
    execution: concurrent
    
    inputPort TestPort {
        location: "socket://localhost:9098" 
        protocol: http{
            format = "json"
        }
        interfaces: ProccessIDInterface
    }

    embed Time as Time
    embed Console as Console
    embed Runtime as Runtime

    main {
        [printId(req)]{
            while (true){
                getProcessId@Runtime( request )( processId )
                println@Console("Hello from process: " + processId)()
                sleep@Time(1000)()
            }
        }
    }
}