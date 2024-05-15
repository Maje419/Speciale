from database import DatabaseInterface
from json-utils import JsonUtils
from reflection import Reflection
from time import Time
from console import Console

from ..publicInboxTypes import InboxConfig

/** This interface is only needed since the InboxReaderService needs to call itself after it has completed its operations */
interface InboxReaderInterface {
    OneWay: 
        /** This operation is the main loop of the InboxReaderService. It reads from the inbox table, forwards any found messages, and deals with deleting them again once they've been handled. */
        beginReading
}

service InboxReaderService (p: InboxConfig){
    execution: concurrent

    /** Used for the InboxReader to call itself */
    inputPort Self {
        Location: "local"
        Interfaces: InboxReaderInterface
    }

    /** Allows for the InboxReader to send messages back to whichever service embeds it */
    outputPort Embedder 
    {
        Location: "local"   // Overwritten in init
    }

    /** The service which can handle transactions. Needed for implementing the inbox pattern. */
    outputPort Database 
    {
        Location: "local"   // Overwritten in init
        Interfaces: DatabaseInterface
    }

    embed Console as Console
    embed JsonUtils as JsonUtils
    embed Reflection as Reflection
    embed Time as Time

    init 
    {
        // This service will forward calls to its embedder service from the inbox
        Embedder.location << p.locations.localLocation
        Database.location << p.locations.databaseServiceLocation

        scope ( AwaitInboxTableCreation ){
            tableCreated = false;
            while (!tableCreated){
                install( SQLException => sleep@Time( p.pollTimer )() )
                query@Database("SELECT * from inbox;")()
                tableCreated = true
            }
        }
        
        scheduleTimeout@Time( p.pollTimer{
                    operation = "beginReading"
            } )(  )
    }

    main
    {
        [beginReading()]{
            // Read any messages left in the 'inbox' table
            query@Database("SELECT * FROM inbox;")( queryResponse );
            for ( row in queryResponse.row )
            {
                println@Console("Inbox: Reading and processing message for operation " + row.operation )()
                
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

                scope ( CatchUserFault ){
                    install (default => {
                        rollbackTx@Database( txHandle )()

                        // At this point, the inbox could enter a 'Hey, something went wrong with this message' message into Kafka
                        // instead of simply retrying

                        scheduleTimeout@Time( p.pollTimer{
                            operation = "beginReading"
                        } )(  )
                    })

                    // Call the corresponding operation at the embedder service
                    with( embedderInvokationRequest )
                    {
                        .outputPort = "Embedder";
                        .data << parameters;
                        .operation = row.operation
                    }

                    invokeRRUnsafe@Reflection( embedderInvokationRequest )()
                    commitTx@Database( txHandle )( ret )
                    println@Console("Inbox: Sucessfully committed update for operation " + row.operation + "\n" )()
                }
            }

            scheduleTimeout@Time( p.pollTimer{
                operation = "beginReading"
            } )(  )
        }
    }
}