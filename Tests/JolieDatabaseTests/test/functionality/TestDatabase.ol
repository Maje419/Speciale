from database import Database, ConnectionInfo
from console import Console

from ..assertions import Assertions

interface TestInterface {
    RequestResponse:

    /// @BeforeAll
    setup_connection(void)(void),

    /// @BeforeEach
    clear_tables(void)(void),

    /// @Test
    can_query_from_the_table(void)(void) throws AssertionError,

    /// @Test
    can_insert_a_new_entry_into_the_table(void)(void) throws AssertionError,
    
    /// @Test
    can_update_an_entry_in_the_table(void)(void) throws AssertionError,

    /// @Test
    can_check_connection(void)(void) throws AssertionError,

    /// @Test
    can_execute_a_transaction(void)(void) throws AssertionError,

    /// @Test
    can_close_and_open_connections(void)(void) throws AssertionError,

    // Transactions
    /// @Test
    can_initialize_new_transaction(void)(void) throws AssertionError,

    /// @Test
    executing_query_on_non_initialized_transaction_throws_exception(void)(void) throws AssertionError,

    /// @Test
    updates_executed_in_an_uncommitted_transaction_are_not_visible(void)(void) throws AssertionError,

    /// @Test
    committing_an_update_results_in_update_becomming_visible(void)(void) throws AssertionError,

    /// @Test
    rolling_back_a_transaction_discards_the_transaction_handle(void)(void) throws AssertionError,

    /// @Test
    rolling_back_a_transaction_discards_any_changes_made_in_it(void)(void) throws AsssertionError,

    /// @Test
    can_provide_hikari_configs(void)(void) throws AssertionError,

    /// @Test
    cannot_access_transaction_after_close(void)(void) throws AssertionError,
    
    /// @Test
    two_different_transactions_get_different_handles(void)(void) throws AssertionError,

    /// @Test
    provoking_a_connection_leak_throws_an_exception(void)(void) throws AssertionError,
}

type TestParams{
    username: string
    password: string
    database: string
    driver: string
    host: string
}

service TestDatabase(p: TestParams){
    execution: sequential
    inputPort Input {
        Location: "local"
        Interfaces: TestInterface
    }

    embed Assertions as Assertions
    embed Console as Console
    embed Database as Database

    main{
        [setup_connection()(){
            println@Console("Connecting to db: " + p.database)()
            connect@Database(p)()
            update@Database("CREATE TABLE IF NOT EXISTS testTable(id INTEGER, testString VARCHAR(50));")()
            if (p.driver == "hsqldb_embedded"){
                update@Database("SET DATABASE TRANSACTION CONTROL MVCC;")()
            }
        }]  

        [clear_tables()(){
            connect@Database(p)()
            update@Database("DELETE FROM testTable WHERE true;")()
            update@Database("INSERT INTO testTable(id, testString) VALUES (1337, 'testUser');")()
        }]

        [can_query_from_the_table()(){
            // Arrange
            s = "SELECT * FROM testTable;"

            // Act
            query@Database(s)(queryResponse)

            // Assert
            equals@Assertions({
                actual = #queryResponse.row
                expected = 1
                message = "Expecteed 1 row but found " + #queryResponse.row
            })()

            equals@Assertions({
                actual = queryResponse.row[0].id
                expected = 1337
                message = "Expected to find id 1337 row but found " + queryResponse.row[0].id
            })()
        }]

        [can_insert_a_new_entry_into_the_table()(){
            // Arrange
            s = "INSERT INTO testTable(id, testString) VALUES (42, 'NewTestUser');"

            // Act
            update@Database(s)(numberRowsAffected)

            // Assert
            equals@Assertions({
                actual = numberRowsAffected
                expected = 1
                message = "Expected the insert to affect 1 row, but found " + numberRowsAffected
            })()
        }]

        [can_update_an_entry_in_the_table()(){
            // Arrange
            s = "UPDATE testTable SET teststring = 'UpdatedUsername' where id = 1337;"

            // Act
            update@Database(s)(numberRowsAffected)

            // Assert
            
            query@Database("SELECT * FROM testTable;")(queryResponse)

            equals@Assertions({
                actual = queryResponse.row[0].testString
                expected = "UpdatedUsername"
                message = "Expected the updated user to have username 'UpdatedUsername', but found " + queryResponse.row[0].testString
            })()
        }]

        [can_check_connection()(){
            // Arrange

            // Act
            checkConnection@Database()()

            // Assert
        }]

        [can_execute_a_transaction()(){
            // Arrange
            with (statements){
                .statement[0] = "INSERT INTO testTable(id, testString) VALUES (99, 'transactionUser');"
                .statement[1] = "DELETE FROM testTable WHERE id = 1337;"
            }

            // Act
            executeTransaction@Database(statements)()

            // Assert
            query@Database("SELECT * FROM testTable;")(queryResponse)
            equals@Assertions({
                actual = #queryResponse.row
                expected = 1
                message = "Expected the table to contain 1 message, but found " + #queryResponse.row
            })()

            equals@Assertions({
                actual = queryResponse.row[0].id
                expected = 99
                message = "Expected the entry to have id 99, but found" + queryResponse.row[0].id
            })()
        }]

        [can_close_and_open_connections()(){
            // Arrange
            newConnection << {
                username = ""
                password = ""
                database = "file:testDatabase"
                driver = "hsqldb_embedded"
                host = "123"
            }

            // Act
            scope (ShouldNotThrow){
                install(SQLException => {
                    connect@Database(p)()
                })
                connect@Database(newConnection)()
            }
            
            // Assert
            if (is_defined(ShouldNotThrow.SQLException)){
                throw AssertionError("Connecting twice went wrong")
            }
        }]

        // Transactions
        [can_initialize_new_transaction()(){
            // Arrange

            // Act
            beginTx@Database()(txHandle)

            // Assert
            if (!is_defined(txHandle)){
                throw AssertionError
            }
        }]

        [executing_query_on_non_initialized_transaction_throws_exception()(){
            // Arrange
            s << 
            {
                query = "SELECT * FROM testTable;"
                txHandle = -12
            }

            // Act
            scope (ShouldThrow){
                install(TransactionException => {x = true})
                query@Database(s)(queryResponse)
            }

            // Assert
            equals@Assertions({
                actual = ShouldThrow.TransactionException
                expected = "TransactionException"
                message = "Expected query to throw TransactionException, but '" + ShouldThrow.TransactionException + "' was thrown"
            })()
        }]

        [updates_executed_in_an_uncommitted_transaction_are_not_visible()(){
            // Arrange
            s << 
            {
                update = "INSERT INTO testTable(id, testString) VALUES (42, 'NewTestUser');"
            }
            beginTx@Database()(s.txHandle)

            // Act
            update@Database(s)(o)

            // Assert
            query@Database("SELECT * FROM testTable;")(queryResponse)

            equals@Assertions({
                actual = #queryResponse.row
                expected = 1
                message = "Expecteed 1 row but found " + #queryResponse.row
            })()

            equals@Assertions({
                actual = queryResponse.row[0].id
                expected = 1337
                message = "Expected to find id 1337 row but found " + queryResponse.row[0].id
            })()
        }]

        [committing_an_update_results_in_update_becomming_visible()(){
            // Arrange
            s << 
            {
                update = "INSERT INTO testTable(id, testString) VALUES (42, 'NewTestUser');"
            }
            beginTx@Database()(s.txHandle)
            update@Database(s)(o)
            query@Database("SELECT * FROM testTable;")(queryPreCommit)

            // Act
            commitTx@Database(s.txHandle)()

            // Assert
            query@Database("SELECT * FROM testTable;")(queryAfterCommit)

            equals@Assertions({
                actual = #queryPreCommit.row
                expected = 1
                message = "Expecteed 1 row after commit but found " + #queryPreCommit.row
            })()

            equals@Assertions({
                actual = #queryAfterCommit.row
                expected = 2
                message = "Expecteed 2 rows after commit but found " + #queryAfterCommit.row
            })()

        }]

        [rolling_back_a_transaction_discards_the_transaction_handle()(){
            // Arrange
            s << 
            {
                update = "INSERT INTO testTable(id, testString) VALUES (42, 'NewTestUser');"
            }
            beginTx@Database()(s.txHandle)
            update@Database(s)(o)

            // Act
            rollbackTx@Database(s.txHandle)()

            // Assert
            query@Database("SELECT * FROM testTable;")(queryAfterCommit)
            
            selectRequest << {
                query = "SELECT * FROM testTable;"
                txHandle = s.txHandle

            }
            
            scope (ShouldThrow){
                install(TransactionException => {x = true})
                query@Database(selectRequest)(queryResponse)
            }

            equals@Assertions({
                actual = ShouldThrow.TransactionException
                expected = "TransactionException"
                message = "Expected a TransactionException when trying to query a rolled-back transaction"
            })()
        }]

        [rolling_back_a_transaction_discards_any_changes_made_in_it()(){
            // Arrange
            s << 
            {
                update = "INSERT INTO testTable(id, testString) VALUES (42, 'NewTestUser');"
            }
            beginTx@Database()(s.txHandle)
            update@Database(s)(o)

            // Act
            rollbackTx@Database(s.txHandle)()

            // Assert
            query@Database("SELECT * FROM testTable;")(queryAfterCommit)
            
            scope (ShouldThrow){
                install(TransactionException => {x = true})
                query@Database("SELECT * FROM testTable;")(queryResponse)
            }

            equals@Assertions({
                actual = #queryResponse.row
                expected = 1
                message = "Expected to find a single enty in the database, but found " + #queryResponse.row
            })()
        }]

        [can_provide_hikari_configs()(){
            // Arrange
            newConnection << p
            newConnection.connectionPoolConfig << {
                readOnly = true
            }
            connect@Database(newConnection)()

            // Act
            scope (ShouldThrow){
                install(SQLException => {x = true})
                update@Database("INSERT INTO testTable(id, testString) VALUES (42, 'NewTestUser');")(o)
            }

            // Assert
            query@Database("SELECT * FROM testTable;")(queryAfterCommit)
            
            selectRequest << {
                query = "SELECT * FROM testTable;"
                txHandle = s.txHandle

            }

            equals@Assertions({
                actual = ShouldThrow.SQLException
                expected = "SQLException"
                message = "Expected updating readonly connection pool to fail"
            })()
        }]

        [cannot_access_transaction_after_close()(){
            // Arrange
            beginTx@Database()(tHandle)
            close@Database()()

            queryRequest << {
                query = "SELECT * FROM testable;"
                txHandle = tHandle
            }

            // Act
            scope (ShouldThrow){
                install(TransactionException => {
                    connect@Database(p)()
                })
                query@Database(queryRequest)()
            }
            
            // Assert
             equals@Assertions({
                actual = ShouldThrow.TransactionException
                expected = "TransactionException"
                message = "Expected a transaction exception but found " + ShouldThrow.TransactionException
            })()
            connect@Database(p)()
        }]

        [provoking_a_connection_leak_throws_an_exception()(){
            // Arrange
            newConnection << p
            newConnection.connectionPoolConfig << {
                maximumPoolSize = 1         // Ensure only one connetion is in the pool
                connectionTimeout = 3000    // Wait 3 seconds for a connection
            }

            connect@Database(newConnection)()
            beginTx@Database()(txHandle)    // Mark the connection as 'busy'

            // Act
            scope (ShouldThrow){
                install (SQLException => {x = true})                
                beginTx@Database()(txHandle)
            }

            // Assert
            equals@Assertions({
                actual = ShouldThrow.SQLException
                expected = "SQLException"
                message = "Expected to be unable to get a connection from the connection pool"
            })()
        }]

        [two_different_transactions_get_different_handles()(){
            // Arrange
            beginTx@Database()(txHandles[0])

            // Act
            beginTx@Database()(txHandles[1])

            // Assert
            notEquals@Assertions({
                actual = txHandles[1]
                expected = txHandles[0]
                message = "Expected to find differing txHandles, but found '" + txHandles[0] + ", " + txHandles[1] + "'"
            })()
        }]

        //TODO:
        // Test that all supported drivers are also supported by HikariCP (They should be)
    }
}