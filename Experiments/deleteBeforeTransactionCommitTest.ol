from console import Console
from time import Time
from database import Database
from .transactionService import TransactionService


interface AInterface {
  RequestResponse: 
    op1(void)(void)
}

service A {
  execution: concurrent

  embed Console as Console
  embed Time as Time
  embed Database as Database
  embed TransactionService as TransactionService
  
  inputPort IP {
    location: "socket://localhost:8087" 
    protocol: http{
        format = "json"
    }
    interfaces: AInterface
  }

  init {
    // connect to DB
        with ( connectionInfo ) {
            .username = "postgres"
            .password = "example"
            .database = "service-a-db"
            .driver = "postgresql"
            .host = ""
        }
        connect@Database( connectionInfo )( void )
        connect@TransactionService( connectionInfo )(void)
        
        update@Database( "DROP TABLE test;" )()
        update@Database( "CREATE TABLE IF NOT EXISTS test(username VARCHAR(50), number INT);" )()
        update@Database( "INSERT INTO test(username, number) VALUES('testUser', 0);" )()
  }

  main {
    [op1( req )( res )]
    {
        query@Database("SELECT * FROM test")( queryResponse )
        println@Console("Rows in table")()
        for ( row in queryResponse.row ){
            println@Console("\t" + row.username)()
        }

        initializeTransaction@TransactionService()(tHandle)

        (
            println@Console("T1: Executing update withing transaction")()
            executeUpdate@TransactionService({.handle = tHandle, .update = "UPDATE test SET number = number + 1 WHERE username = 'testUser' "})()
            println@Console("T1: Sleeping for 10 seconds")()
            sleep@Time(10000)()
            println@Console("T1: Committing")()
            commit@TransactionService(tHandle)()
            |
            println@Console("T1: Sleeping for 1 second")()
            sleep@Time(1000)()
            println@Console("T1: Executing update")()
            update@Database("UPDATE test SET number = number + 1 WHERE username = 'testUser'")()
        )

        query@Database("SELECT * FROM test")( queryResponse )
        println@Console("Rows in table:")()
        for ( row in queryResponse.row ){
            println@Console("\t" + row.username + ": " + row.number)()
        }
        
    }
  }
}