from console import Console
from time import Time

type Input {
  .input: string
}

interface AInterface {
  RequestResponse: 
    op1(Input)(void)
}

service A {

  embed Console as Console
  embed Time as Time

  main {
      r.one = "hello"
      r.two = "hi"
      
      println@Console(
        if (false)
        {
          lol -> r.one;

        } else {
          lol -> r.two;
        }
       )()

      
    }
}