from ...SafeConsumer.serviceB import ServiceB
from ..assertions import Assertions

from runtime import Runtime
from reflection import Reflection
from time import Time
from console import Console
from database import Database

interface ServiceBFailureInterface {
    RequestResponse:
        /// @BeforeAll
        connect_to_db(void)(void),

        // /// @Test
        // Services_are_out_of_sync_if_B_throws_before_updating_local(void)(void) throws AssertionError(string),
        
        // /// @Test
        // Services_remain_in_sync_if_B_throws_after_updating_local(void)(void) throws AssertionError(string),

        // /// @Test
        // Services_are_out_of_sync_if_MRS_throws_before_forwarding_message_to_B(void)(void) throws AssertionError(string),

        // /// @Test
        // Services_remain_in_sync_when_MRS_throws_after_messaging_B_and_a_single_message_was_sent(void)(void) throws AssertionError(string),

        // /// @Test
        // Services_are_out_of_sync_if_MRS_throws_before_recursing(void)(void) throws AssertionError(string),

        /// @AfterEach
        clear_tables(void)(void)
}

service ServiceBFailure{
    execution: sequential

    inputPort TestInterface {
        Location: "local"
        Interfaces: ServiceBFailureInterface 
    }
    
    embed Assertions as Assertions
    embed Database as Database
    embed Time as Time
    embed Runtime as Runtime
    embed Console as Console
    embed Reflection as Reflection
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

        //  -------------- Service B Tests:
        
        [Services_are_out_of_sync_if_B_throws_before_updating_local()(){
            println@Console("\n\n------------------ Executing 'Services_are_out_of_sync_if_B_throws_before_updating_local' ------------------")()
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

            sleep@Time(5000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            // If B crashes before having updated itself, A and B might be out of sync
            println@Console("Expecting A: " + rowsA.row[0].number + " to equal B: " + rowsB.row[0].number)()
            equals@Assertions({
                actual = rowsB.row[0].number + 1
                expected = rowsA.row[0].number
            })()
        }]

        [Services_remain_in_sync_if_B_throws_after_updating_local()(){
            println@Console("\n\n------------------ Executing 'Services_remain_in_sync_if_B_throws_after_updating_local' ------------------")()
            
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

            sleep@Time(5000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            println@Console("Expecting A: " + rowsA.row[0].number + " to equal B: " + rowsB.row[0].number)()
            equals@Assertions({
                actual = rowsB.row[0].number
                expected = rowsA.row[0].number
            })()
        }]

        [Services_are_out_of_sync_if_MRS_throws_before_forwarding_message_to_B()(){
            println@Console("\n\n------------------ Executing 'Services_are_out_of_sync_if_MRS_throws_before_forwarding_message_to_B' ------------------")()
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
                        .throw_after_updating_main_service = false
                        .throw_before_scheduling_timeout = false
                    }
                }
            }

            setupTest@ServiceA(testScenario)( )
            setupTest@ServiceB(testScenario)( )

            // Act
            updateNumber@ServiceA({.username = "user1"})()

            sleep@Time(5000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            // Since B was never updated, A should be one larger than B
            println@Console("Expecting A: " + rowsA.row[0].number + " to equal B: " + rowsB.row[0].number)()
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = rowsB.row[0].number + 1
            })()
        }]

        [Services_remain_in_sync_when_MRS_throws_after_messaging_B_and_a_single_message_was_sent()(){
            // Note that the result of this test would be very different if multiple messages were put into kafka before B was initialized.
            // In such a case, MRS would only forward the first message to B, and the rest would be dropped. Testing this requires me to
            // Make a Kafka service which allowes for manipulating a desired topic, which is a TODO
            
            println@Console("\n\n------------------ Executing 'Services_remain_in_sync_when_MRS_throws_after_messaging_B_and_a_single_message_was_sent' ------------------")()
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
                        .throw_on_message_found = false
                        .throw_after_updating_main_service = true
                        .throw_before_scheduling_timeout = false
                    }
                }
            }

            setupTest@ServiceA(testScenario)( )
            setupTest@ServiceB(testScenario)( )

            // Act
            updateNumber@ServiceA({.username = "user1"})()

            sleep@Time(5000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            // Since B was never updated, A should be one larger than B
            println@Console("Expecting A: " + rowsA.row[0].number + " to equal B: " + rowsB.row[0].number)()
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = rowsB.row[0].number
            })()
        }]

        // The below test needs to go into its own file, it is very flaky, since it's 'random' whether the MRS crashes before 
        /*[Services_are_out_of_sync_if_MRS_throws_before_recursing()(){
            // Note that the result of this test would be very different if multiple messages were put into kafka before B was initialized.
            // In such a case, MRS would only forward the first message to B, and the rest would be dropped. Testing this requires me to
            // Make a Kafka service which allowes for manipulating a desired topic, which is a TODO
            
            println@Console("\n\n------------------ Executing 'Services_are_out_of_sync_if_MRS_throws_before_recursing' ------------------")()
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
                        .throw_on_message_found = false
                        .throw_after_updating_main_service = false
                        .throw_before_scheduling_timeout = true
                    }
                }
            }

            setupTest@ServiceA(testScenario)( )
            setupTest@ServiceB(testScenario)( )

            // Act
            updateNumber@ServiceA({.username = "user1"})()

            sleep@Time(5000)()

            //Assert
            query@Database("SELECT * FROM NumbersA")(rowsA)
            query@Database("SELECT * FROM NumbersB")(rowsB)

            // Since B was never updated, A should be one larger than B
            println@Console("Expecting A: " + rowsA.row[0].number + " to equal B: " + rowsB.row[0].number)()
            equals@Assertions({
                actual = rowsA.row[0].number
                expected = rowsB.row[0].number + 1
            })()
        }]*/
    }
}