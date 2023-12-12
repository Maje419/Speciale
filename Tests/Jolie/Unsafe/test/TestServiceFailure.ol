from ..UnsafeProducer.serviceA import ServiceA
from ..UnsafeConsumer.serviceB import ServiceB

from assertions import Assertions
from runtime import Runtime
from reflection import Reflection
from time import Time
from console import Console
from database import Database

interface TestServiceFailureInterface {
    RequestResponse:
        /// @BeforeAll
        connect_to_db(void)(void),

        /// @Test
        A_failure_before_updating_local(void)(void) throws AssertionError(string),

        /// @Test
        A_failure_before_forwarding_update(void)(void) throws AssertionError(string),

        /// @Test
        A_failure_after_forwarding_update(void)(void) throws AssertionError(string),

        /// @Test
        B_failure_before_updating_local(void)(void) throws AssertionError(string),
        
        /// @Test
        B_failure_after_updating_local(void)(void) throws AssertionError(string),

        /// @Test
        MRS_throw_on_message_received(void)(void) throws AssertionError(string),

        /// @AfterEach
        clear_tables(void)(void)
}

service TestServiceFailure{
    execution: sequential

    inputPort TestInterface {
        Location: "local"
        Interfaces: TestServiceFailureInterface 
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
        //  -------------- Setup and teardown tests:
        [connect_to_db()(){
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

            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            update@Database("DELETE FROM NumbersA WHERE true;")()
            update@Database("DELETE FROM NumbersB WHERE true;")()
            update@Database("INSERT INTO NumbersA VALUES('user1', 0);")()
            update@Database("INSERT INTO NumbersB VALUES('user1', 0);")()
        }]

        [clear_tables()(){
            println@Console("\n\n------------------ Clearing tables for next test ------------------")()
            sleep@Time(5000)()
            update@Database("DELETE FROM NumbersA WHERE true;")()
            update@Database("DELETE FROM NumbersB WHERE true;")()
            update@Database("INSERT INTO NumbersA VALUES('user1', 0);")()
            update@Database("INSERT INTO NumbersB VALUES('user1', 0);")()

            undef(rowsA)
            undef(rowsB)
        }]

        //  -------------- Service A Tests:

        [A_failure_before_updating_local()(){
            println@Console("\n\n------------------ Executing 'A_failure_before_updating_local' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  true,
                    .throw_before_sending= false,
                    .throw_after_sending = false
                }
            }

            setupTest@ServiceA(testScenario)(_)
            setupTest@ServiceB(testScenario)( )

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

        [A_failure_before_forwarding_update()(){
            println@Console("\n\n------------------ Executing 'A_failure_before_forwarding_update' ------------------")()
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
            setupTest@ServiceB(testScenario)( )
            
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
    
        [A_failure_after_forwarding_update()(){
            println@Console("\n\n------------------ Executing 'A_failure_after_forwarding_update' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  false,
                    .throw_before_sending= false,
                    .throw_after_sending = true
                }
            }

            setupTest@ServiceA(testScenario)(_)
            setupTest@ServiceB(testScenario)( )

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

        //  -------------- Service B Tests:
        
        [B_failure_before_updating_local()(){
            println@Console("\n\n------------------ Executing 'B_failure_before_updating_local' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  false,
                    .throw_before_sending= false,
                    .throw_after_sending = false
                }
                .serviceB << {
                    .throw_before_updating_local = true
                    .throw_after_updating_local = false
                }
            }

            setupTest@ServiceA(testScenario)( )
            setupTest@ServiceB(testScenario)( )

            // Act
            updateNumber@ServiceA({.username = "user1"})()

            sleep@Time(10000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            // If B crashes before having updated itself, A and B might be out of sync
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = 0
            })()

            equals@Assertions({
                actual = rowsB.row[0].number
                expected = rowsA.row[0].number
            })()
        }]

        [B_failure_after_updating_local()(){
            println@Console("\n\n------------------ Executing 'B_failure_after_updating_local' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  false,
                    .throw_before_sending= false,
                    .throw_after_sending = false
                }
                .serviceB << {
                    .throw_before_updating_local = false
                    .throw_after_updating_local = true
                }
            }

            setupTest@ServiceA(testScenario)( )
            setupTest@ServiceB(testScenario)( )

            // Act
            updateNumber@ServiceA({.username = "user1"})()

            sleep@Time(10000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            // If B crashes before having updated itself, A and B might be out of sync
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = 0
            })()

            equals@Assertions({
                actual = rowsB.row[0].number
                expected = rowsA.row[0].number
            })()
        }]

        [MRS_throw_on_message_received()(){
            println@Console("\n\n------------------ Executing 'MRS_throw_on_message_received' ------------------")()
            // Arrange:
            testScenario << {
                .serviceA << {
                    .throw_before_updating_local =  false
                    .throw_before_sending= false
                    .throw_after_sending = false
                }
                .serviceB << {
                    .throw_before_updating_local = false
                    .throw_after_updating_local = false
                    .mrs << {
                        .throw_on_message_found = true
                        .throw_before_updating_main_service = false
                        .throw_after_updating_main_service = false
                        .throw_before_scheduling_timeout = false
                    }
                }
            }

            setupTest@ServiceA(testScenario)( )
            setupTest@ServiceB(testScenario)( )

            // Act
            updateNumber@ServiceA({.username = "user1"})()

            sleep@Time(10000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            // If B crashes after having updated itself, A and B should not be desynced
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = 0
            })()

            equals@Assertions({
                actual = rowsB.row[0].number
                expected = rowsA.row[0].number
            })()
        }]
    }
}