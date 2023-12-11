from ..UnsafeProducer.serviceA import ServiceA
from ..UnsafeConsumer.serviceB import ServiceB

from assertions import Assertions
from runtime import Runtime
from reflection import Reflection
from time import Time
from console import Console
from database import Database

interface TestServiceAFailureInterface {
    RequestResponse:
        /// @BeforeAll
        loadEmbeddedAndConnectToDb(void)(void),

        /// @Test
        failure_before_updating_local(void)(void) throws AssertionError(string),

        /// @Test
        failure_before_forwarding_update(void)(void) throws AssertionError(string),

        /// @Test
        failure_after_forwarding_update(void)(void) throws AssertionError(string),

        /// @AfterEach
        clearTables(void)(void)
}

service TestServiceAFailure{
    execution: sequential

    inputPort TestInterface {
        Location: "local"
        Interfaces: TestServiceAFailureInterface 
    }
    
    embed Assertions as Assertions
    embed Database as Database
    embed Time as Time
    embed Runtime as Runtime
    embed Console as Console
    embed Reflection as Reflection
    embed ServiceA as ServiceA
    embed ServiceB as ServiceB

    main
    {
        [loadEmbeddedAndConnectToDb()(){
            // Connect to db
            println@Console("\n\n------------------ Connecting to Database ------------------")()
            with ( connectionInfo ) 
            {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
            };
            connect@Database(connectionInfo)()

            loadEmbeddedService@Runtime({
                .filepath = "../UnsafeConsumer/serviceB.ol"
            })( )
        }]

        [clearTables()(){
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM NumbersA WHERE true;")()
            update@Database("DELETE FROM NumbersB WHERE true;")()
            update@Database("INSERT INTO NumbersA VALUES('user1', 0);")()
            update@Database("INSERT INTO NumbersB VALUES('user1', 0);")()

            undef(rowsA)
            undef(rowsB)
        }]

        [failure_before_updating_local()(){
            println@Console("\n\n------------------ Executing 'failure_before_updating_local' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  true,
                    .throw_before_sending= false,
                    .throw_after_sending = false
                }
            }

            setupTest@ServiceA(testScenario)(_)

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                updateNumber@ServiceA({.username = "user1"})()
            }

            // sleep@Time(10000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            equals@Assertions({
                actual = ExecuteUpdate.TestException
                expected = "throw_before_updating"
            })()

            // If A crashes before having updated itself, no update should take place 
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = 0
            })()

            equals@Assertions({
                actual = rowsB.row[0].number
                expected = rowsA.row[0].number
            })()
        }]

        [failure_before_forwarding_update()(){
            println@Console("\n\n------------------ Executing 'failure_before_forwarding_update' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  false,
                    .throw_before_sending= true,
                    .throw_after_sending = false
                }
            }

            with (loadEmbeddedRequest){
                .filepath = "../UnsafeProducer/serviceA.ol";
                .params << testScenario 
            }

            setupTest@ServiceA(testScenario)(_)

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                updateNumber@ServiceA({.username = "user1"})()
            }
            sleep@Time(5000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            equals@Assertions({
                actual = ExecuteUpdate.TestException
                expected = "throw_before_sending"
            })()

                // If using the outbox pattern, we expect no updates to have taken place in A in this case
            println@Console("Expecting A: " + #rowsA.row[0].number + " to equal B: " + #rowsB.row[0].number)()
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = rowsB.row[0].number
            })()
        }]
    
        [failure_after_forwarding_update()(){
            println@Console("\n\n------------------ Executing 'failure_after_forwarding_update' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  false,
                    .throw_before_sending= false,
                    .throw_after_sending = true
                }
            }

            setupTest@ServiceA(testScenario)(_)

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                updateNumber@ServiceA({.username = "user1"})()
            }

            sleep@Time(10000)()
            
            // Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            equals@Assertions({
                actual = ExecuteUpdate.TestException
                expected = "throw_after_sending"
            })()

                // If the service crashes after having messaged B, we expect the message to reach B
            equals@Assertions({
                actual = rowsB.row[0].number
                expected = rowsA.row[0].number
            })()
        }]
    }
}