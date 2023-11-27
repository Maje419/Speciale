from database import Database
from json-utils import JsonUtils
from reflection import Reflection
from time import Time

from .inboxTypes import InboxEmbeddingConfig
from ..TransactionService.transactionService import TransactionServiceOperations

service InboxReaderService (p: InboxEmbeddingConfig){
    execution: single

    embed Database as Database
    embed JsonUtils as JsonUtils
    embed Reflection as Reflection
    embed Time as Time

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
        connect@Database(p.databaseConnectionInfo)()
    }

    main
    {
        while (true)
        {
            query@Database("SELECT * FROM inbox;")( queryResponse );
            for ( row in queryResponse.rows )
            {
                install ( TransactionClosedFault  => 
                    {
                        // This exception is thrown if the query above includes a message whose transaction is
                        // closed before we manage to abort it.
                        // In this case, we don't want to reopen a transaction for said message.
                        s = 12  // TODO: This is a temp assignment
                    } )
                
                // If we already have an open transaction for some inbox message,
                // it might have been lost or some service might have crashed before commiting.
                // We will abort the transaction and attempt again.
                if (is_defined(global.openTransactions.(row.kafkaId + ":" + row.messageId)))
                {
                    tHandle = global.openTransactions.(row.kafkaId + ":" + row.messageId)
                    abort@TransactionService( tHandle )( aborted )
                    if ( !aborted )
                    {
                        throw ( TransactionClosedFault )
                    }
                }

                // Initialize a new transaction to pass onto Service A
                initializeTransaction@TransactionService()(tHandle)
                global.openTransactions.(row.kafkaId + ":" + row.messageId) = tHandle;

                // Within this new transaction, update the current message as read
                with( updateRequest )
                {
                    .handle = tHandle;
                    .update = "DELETE FROM inbox WHERE " + 
                            "kafakaId = " + row.kafakaId + 
                            " AND messageId = " + row.messageId + ";"
                }

                executeUpdate@TransactionService( updateRequest )()
                
                // The operation request is stored in the "parameters" column
                getJsonValue@JsonUtils( row.parameters )( parameters )
                finalizeRequest.handle = tHandle

                // Call the corresponding operation at the embedder service
                with( embedderInvokationRequest )
                {
                    .outputPort = "Embedder";
                    .data << parameters;
                    .operation = row.operation
                }

                invoke@Reflection( embedderInvokationRequest )()
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
                    if ( row.kafkaId + ":" + row.messageId == transaction )
                    {
                        transactionIsClosed = false
                    }
                }
                if ( transactionIsClosed )
                {
                    undef( global.openTransactions.transaction )
                }
            }

            // Sleep for 10 seconds
            sleep@Time( 10000 )()
        }

    }
}