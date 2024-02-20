from console import Console
from time import Time

interface AInterface {
  RequestResponse: 
    setupTest( string )( bool ),
    op1(void)(void)

  OneWay:
    op2(bool)
          
}

service A {
  execution: concurrent

  embed Console as Console
  embed Time as Time
  
  inputPort IP {
    location: "local"
    interfaces: AInterface
  }

  main {
    [op1()()]
    {
      while(true){
          println@Console("TestParam: " + global.testParam)()
          sleep@Time(100)()
        }
    }

    [setupTest( req )( res ){
      global.testParam << req
      println@Console("Setting testparam to: " + req)()
      res = true
    }]
  }
}