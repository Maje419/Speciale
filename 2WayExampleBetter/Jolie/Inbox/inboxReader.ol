from database import DatabaseInterface
from json-utils import JsonUtils
from reflection import Reflection
from time import Time
from console import Console

from .inboxTypes import InboxConfig, InboxReaderInterface
from ..ServiceA.serviceAInterface import ServiceAInterfaceLocal

interface InboxReaderInterface {
    OneWay: 
        beginReading
}

service InboxReaderService (p: InboxConfig){
    execution: concurrent

    embed Console as Console
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

    outputPort Database 
    {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: DatabaseInterface
    }

    init 
    {
        // This service will forward calls to its embedder service from the inbox
        Embedder.location << p.localLocation
        Database.location << p.databaseServiceLocation
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
    }

    main
    {
        [beginReading()]{
            query@Database("SELECT * FROM inbox;")( queryResponse );
            for ( row in queryResponse.row )
            {
                println@Console("Inbox: Reading and processing message for operation " + row.key )()
                // Initialize a new transaction to pass onto Service A
                beginTx@Database()(txHandle)

                // Within this new transaction, update the current message as read
                with( updateRequest )
                {
                    .txHandle = txHandle;
                    .update = "DELETE FROM inbox WHERE arrivedFromKafka = " + row.arrivedfromkafka + 
                            " AND messageId = '" + row.messageid + "';"
                }

                update@Database( updateRequest )()
                
                // The operation request is stored in the "parameters" column
                getJsonValue@JsonUtils( row.parameters )( parameters )
                parameters.txHandle = txHandle

                // Call the corresponding operation at the embedder service
                with( embedderInvokationRequest )
                {
                    .outputPort = "Embedder";
                    .data << parameters;
                    .operation = row.operation
                }

                scope ( CatchUserFault ){
                    install (default => {
                        rollbackTx@Database( txHandle )()

                        // At this point, the inbox could enter a 'Hey, something went wrong with this message' message into Kafka
                        // instead of simply retrying

                        scheduleTimeout@Time( 1000{
                            operation = "beginReading"
                        } )(  )
                    })

                    invokeRRUnsafe@Reflection( embedderInvokationRequest )()
                    commitTx@Database( txHandle )( ret )
                    println@Console("Inbox: Sucessfully committed update for operation " + row.key + "\n" )()
                }
            }

            scheduleTimeout@Time( 1000{
                operation = "beginReading"
            } )(  )
        }
    }
}