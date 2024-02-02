from .test2 import A
from time import Time
from console import Console

service B {
  embed Console as Console
  embed Time as Time
  // embed A as A

  main {
    /* Testing concurrecy btwn. A and B */
    // op1@A()()
    // sleep@Time(500)()
    // setupTest@A("Test1")(ret)
    // sleep@Time(500)()
    // setupTest@A("Test2")(ret)
    // sleep@Time(500)()

    /* Testing how scoping works */
    // scope (A){
    //   install (default => {
    //     println@Console("Default")()
    //   })

    //   install (TestException => {
    //     println@Console("TestException")()
    //   })

    //   throw(TestException, "123")

  }
}