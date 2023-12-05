from ..UnsafeProducer.serviceA import ServiceAInterface

from assertions import Assertions
from runtime import Runtime
from reflection import Reflection
from time import Time
from console import Console
from database import Database

interface TestServiceAFailureInterface {
    RequestResponse:
        /// @BeforeAll
        connectToDb(void)(void),

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
        
    outputPort ServiceUnderTest {
        Location: "local"
        Interfaces: ServiceAInterface
    }

    embed Assertions as Assertions
    embed Database as Database
    embed Time as Time
    embed Runtime as Runtime
    embed Console as Console
    embed Reflection as Reflection

    main
    {
        [connectToDb()(){
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
                .throw_before_updating_local =  true,
                .throw_before_sending= false,
                .throw_after_sending = false
            }

            loadEmbeddedService@Runtime({
                .filepath = "../UnsafeProducer/serviceA.ol"
                .params << testScenario 
            })(serviceLocation)

            loadEmbeddedService@Runtime({
                .filepath = "../UnsafeConsumer/serviceB.ol"
                .params << testScenario 
            })( )

            setOutputPort@Runtime({
                location = "local",
                name = "TestOneOPP",
                location << serviceLocation
            })()

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                
                invoke@Reflection({
                    outputPort = "TestOneOPP"
                    data << {.username = "user1"}
                    operation = "updateNumber"
                })(response)
            }

            sleep@Time(10000)()

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
                .throw_before_updating_local =  false,
                .throw_before_sending= true,
                .throw_after_sending = false
            }

            with (loadEmbeddedRequest){
                .filepath = "../UnsafeProducer/serviceA.ol";
                .params << testScenario 
            }

            loadEmbeddedService@Runtime(loadEmbeddedRequest)(serviceLocation)

            loadEmbeddedService@Runtime({
                .filepath = "../UnsafeConsumer/serviceB.ol"
                .params << testScenario 
            })( )

            setOutputPort@Runtime({
                location = "local",
                name = "TestTwoOPP",
                location << serviceLocation
            })()

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                
                invoke@Reflection({
                    outputPort = "TestTwoOPP"
                    data << {.username = "user1"}
                    operation = "updateNumber"
                })(response)
            }
            sleep@Time(10000)()

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
                .throw_before_updating_local =  false,
                .throw_before_sending= false,
                .throw_after_sending = true
            }

            with (loadEmbeddedRequest){
                .filepath = "../UnsafeProducer/serviceA.ol";
                .params << testScenario 
            }

            loadEmbeddedService@Runtime(loadEmbeddedRequest)(serviceLocation)
            loadEmbeddedService@Runtime({
                .filepath = "../UnsafeConsumer/serviceB.ol"
                .params << testScenario 
            })( )

            setOutputPort@Runtime({
                location = "local",
                name = "TestThreeOPP",
                location << serviceLocation
            })()

            // Act
            scope ( ExecuteUpdate ){
                install( TestException => println@Console("Exception: " + ExecuteUpdate.TestException)() )
                
                invoke@Reflection({
                    outputPort = "TestThreeOPP"
                    data << {.username = "user1"}
                    operation = "updateNumber"
                })(response)
            }

            sleep@Time(100000)()
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