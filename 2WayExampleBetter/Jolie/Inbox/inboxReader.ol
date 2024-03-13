from database import Database
from json-utils import JsonUtils
from reflection import Reflection
from time import Time
from console import Console

from .inboxTypes import InboxConfig, InboxReaderInterface
from ..ServiceA.serviceAInterface import ServiceAInterfaceLocal
from ..TransactionService.transactionService import TransactionServiceOperations

interface InboxReaderInterface {
    OneWay: 
        beginReading
}

service InboxReaderService (p: InboxConfig){
    execution: concurrent

    embed Console as Console
    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Reflection as Reflection
    embed Time as Time

    inputPort Self {
        Location: "local"
        Interfaces: InboxReaderInterface
    }
    
    outputPort Embedder 
    {
        Location: "local"   // Overwritten in init
        Interfaces: 
            ServiceAInterfaceLocal
    }

    outputPort TransactionService 
    {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: TransactionServiceOperations
    }

    init 
    {
        // This service will forward calls to its embedder service from the inbox
        Embedder.location << p.localLocation
        TransactionService.location << p.transactionServiceLocation
        connect@Database( p.databaseConnectionInfo )()
        scope ( AwaitInboxTableCreation ){
            tableCreated = false;
            while (!tableCreated){
                install( SQLException => sleep@Time( 1000 )() )
                query@Database("SELECT * from inbox;")()
                tableCreated = true
            }
        }
        
        scheduleTimeout@Time( 1000{
                    operation = "beginReading"
            } )(  )

        println@Console("InboxReader initialized")()
    }

    main
    {
        [beginReading()]{
            query@Database("SELECT * FROM inbox;")( queryResponse );
            println@Console("InboxReader found " + #queryResponse.row + " rows in the inbox")()
            for ( row in queryResponse.row )
            {
                // Initialize a new transaction to pass onto Service A
                initializeTransaction@TransactionService()(tHandle)

                // Within this new transaction, update the current message as read
                with( updateRequest )
                {
                    .handle = tHandle;
                    .update = "DELETE FROM inbox WHERE arrivedFromKafka = " + row.arrivedfromkafka + 
                            " AND messageId = '" + row.messageid + "';"
                }

                executeUpdate@TransactionService( updateRequest )()
                
                // The operation request is stored in the "parameters" column
                getJsonValue@JsonUtils( row.parameters )( parameters )
                println@Console("Parameters username: " + parameters.username)()
                parameters.handle = tHandle

                // Call the corresponding operation at the embedder service
                with( embedderInvokationRequest )
                {
                    .outputPort = "Embedder";
                    .data << parameters;
                    .operation = row.operation
                }

                scope ( CatchUserFault ){
                    println@Console("InboxReader trying to catch user fault")()
                    install (default => {
                        commit@TransactionService( tHandle )()
                        scheduleTimeout@Time( 1000{
                            operation = "beginReading"
                        } )(  )
                    })

                    invokeRRUnsafe@Reflection( embedderInvokationRequest )()
                    commit@TransactionService( tHandle )( ret )
                }
            }

            scheduleTimeout@Time( 1000{
                            operation = "beginReading"
            } )(  )
        }
    }
}