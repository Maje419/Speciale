from database import Database
from json-utils import JsonUtils
from reflection import Reflection
from time import Time
from console import Console

from .inboxTypes import InboxEmbeddingConfig
from ..TransactionService.transactionService import TransactionServiceOperations
from ...test.testTypes import TestInterface

interface InboxReaderInterface {
    OneWay:
        beginReading
}

service InboxReaderService (p: InboxEmbeddingConfig){
    execution: concurrent

    embed Console as Console
    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Reflection as Reflection
    embed Time as Time

    inputPort Self {
        Location: "local"
        Interfaces:
            InboxReaderInterface,
            TestInterface
    }

    outputPort Embedder
    {
        Location: "local"   // Overwritten in init
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

        scheduleTimeout@Time( 10{
                    operation = "beginReading"
            } )(  )

        println@Console("InboxReader initialized")()
    }

    main
    {
        [beginReading()]{
            install (default => {
                println@Console("Default exception caught")()
                scheduleTimeout@Time( 1000{
                    operation = "beginReading"
                } )(  )
            })

            query@Database("SELECT * FROM inbox;")( queryResponse );
            for ( row in queryResponse.row )
            {
                println@Console("Handling row: " + row.messageid)()
                install ( TransactionClosedFault  =>
                    {
                        // This exception is thrown if the query above includes a message whose transaction is
                        // closed before we manage to abort it.
                        // In this case, we don't want to reopen a transaction for said message.
                        println@Console("TransactionClosedFault caught")()
                        undef(global.openTransactions.(row.arrivedfromkafka + ":" + row.messageid))
                    } )

                // If we already have an open transaction for some inbox message,
                // it might have been lost or some service might have crashed before commiting.
                // We will abort the transaction and attempt again.
                if (is_defined(global.openTransactions.(row.arrivedfromkafka + ":" + row.messageid)))
                {
                    tHandle = global.openTransactions.(row.arrivedfromkafka + ":" + row.messageid)
                    println@Console("Attempting to abort")()
                    abort@TransactionService( tHandle )( aborted )
                    if ( !aborted )
                    {
                        println@Console("Connection already aborted!")()
                        // We enter here if the transaction is committed by some other service
                        // before this abort reaches the transaction service
                        throw ( TransactionClosedFault )
                    } else {
                        undef(global.openTransactions.(row.arrivedfromkafka + ":" + row.messageid))
                    }
                }

                // Initialize a new transaction to pass onto Service A
                initializeTransaction@TransactionService()(tHandle)
                global.openTransactions.(row.arrivedfromkafka + ":" + row.messageid) = tHandle;

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
                parameters.handle = tHandle

                // Call the corresponding operation at the embedder service
                with( embedderInvokationRequest )
                {
                    .outputPort = "Embedder";
                    .data << parameters;
                    .operation = row.operation
                }

                invokeRRUnsafe@Reflection( embedderInvokationRequest )()
            }

            // Now we do some cleanup in the global "opentransaction" variable, just for lolz
            // We do this by finding all the openTransactions which do not match a message in the inbox table,
            // meaning they've been committed and deleted.
            for ( transaction in global.openTransactions )
            {
                transactionIsClosed = true
                for ( row in queryResponse.rows )
                {
                    // If some message in the database matches the message in this open transaction,
                    // clearly the trnasaction is not closed yet.
                    if ( row.arrivedfromkafka + ":" + row.messageid == transaction )
                    {
                        transactionIsClosed = false
                    }
                }
                if ( transactionIsClosed )
                {
                    undef( global.openTransactions.transaction )
                }
            }

            // Wait for 1 second, then start from begining
            scheduleTimeout@Time(1000{
                    operation = "beginReading"
            } )(  )

        }

        [setupTest( request )( response ){
            println@Console("InboxReader tests")()

            global.testParams << request.inboxReaderTests
            global.hasThrown = false
            response = true
        }]

    }
}