from database import Database
from json-utils import JsonUtils
from reflection import Reflection
from time import Time
from console import Console

from .inboxTypes import InboxConfig, InboxReaderInterface
from ..ServiceA.serviceAInterface import ServiceAInterfaceLocal
from ..TransactionService.transactionService import TransactionServiceOperations

/** This interface is only needed since the InboxReaderService needs to call itself after it has completed its operations */
interface InboxReaderInterface {
    OneWay:
        /** This operation is the main loop of the InboxReaderService. It reads from the inbox table, forwards any found messages, and deals with deleting them again once they've been handled. */
        beginReading
}

service InboxReaderService (p: InboxConfig){
    execution: concurrent

    embed Console as Console
    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Reflection as Reflection
    embed Time as Time

    /** Used for the InboxReader to call itself */
    inputPort Self {
        Location: "local"
        Interfaces: InboxReaderInterface
    }
    
    /** Allows for the InboxReader to send messages back to whichever service embeds it */
    outputPort Embedder 
    {
        Location: "local"   // Overwritten in init
        Interfaces: 
            ServiceAInterfaceLocal
    }

    /** The service which can handle transactions. Needed for implementing the inbox pattern. */
    outputPort TransactionService 
    {
        Location: "local"   // Overwritten in init
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
            // Read any messages left in the 'inbox' table
            query@Database("SELECT * FROM inbox;")( queryResponse );
            for ( row in queryResponse.row )
            {
                // Initialize a new transaction to pass onto whichever service embeds this one
                initializeTransaction@TransactionService()(tHandle)

                // Within this new transaction, it is safe to delete this message, since if the transaction is committed, the message will have been handled correctly
                with( updateRequest )
                {
                    .handle = tHandle;
                    .update = "DELETE FROM inbox WHERE arrivedFromKafka = " + row.arrivedfromkafka + 
                            " AND messageId = '" + row.messageid + "';"
                }
                executeUpdate@TransactionService( updateRequest )()
                
                // The operation request is stored in the "parameters" column
                getJsonValue@JsonUtils( row.parameters )( parameters )
                parameters.handle = tHandle

                // Call the corresponding operation at the embedder service
                with( embedderInvokationRequest )
                {
                    .outputPort = "Embedder";
                    .data << parameters;
                    .operation = row.operation
                }

                scope ( CatchUserFault ){
                    /*
                            TODO: We have to add some error-handling to this scenario, meaning returning the error on some
                            error channel (topic?).

                            This should either be done in the same transaction, which should then be committed, or in a new update, and the transaction should be aborted.
                            I'm leaning towards a new update, and aborting the failed transaction, as there is no real way or us to know
                                where the transaction failed. This means that committing it, we might commit some queries, but not all, and might
                                therefore leave an invalid state. Better to just abort it.

                            We have then introduced the risk that this service crashes before pushing the message to the error-handling topic, but
                            this is not really a problem, since aborting the transaction means the message causing the problem is still in the inbox, and
                            no information is therefore lost.
                        */
                    install (default => {
                        println@Console("InboxReader trying to catch user fault")()
                        abortTransaction@TransactionService( tHandle )( abortRes )

                        // By using a new transaction to delete the message from the inbox and insert an error message in the outbox, we either handle the error,
                        // or don't delete the message from the inbox. No information is lost
                        initializeTransaction@TransactionService()(recoveryHandle)
                        // executeUpdate@TransactionService({
                        //     .handle = recoveryHandle;
                        //     .update = "DELETE FROM inbox WHERE arrivedFromKafka = " + row.arrivedfromkafka + 
                        //     " AND messageId = '" + row.messageid + "';"
                        //     })()
                        
                        // update@OutboxService({.topic = p.failureTopic; .message = "failed to handle message with messageId 'row.mid'"})()
                        // commit@TransactionService(recoveryHandle)()
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