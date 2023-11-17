include "database.iol"
include "console.iol"
include "time.iol"
include "file.iol"
include "string_utils.iol" 
include "serviceAInterface.iol"

from runtime import Runtime
from .Outbox.outboxService import Outbox
from .TransactionService.transactionService import TransactionService

service ServiceA{
    execution: concurrent
    inputPort ServiceALocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }
    embed Outbox as OutboxService
    embed TransactionService as TransactionService
    embed Runtime as Runtime

    init
    {
        // serviceAConfig.json contains info for connecting to db, as well as kafka settings
        readFile@File(
            {
                filename = "serviceAConfig.json"
                format = "json"
            }) ( config )

        
        // Load the inbox service as an embedded service
        getLocalLocation@Runtime()( location )
        loadEmbeddedService@Runtime( { 
            filepath = "Inbox/inboxServiceA.ol"
            params << { 
                localLocation << location
                externalLocation << "socket://localhost:8080"       //This doesn't work (yet)
                databaseConnectionInfo << config.serviceAConnectionInfo
                transactionServiceLocation << TransactionService.location   // All embedded services must talk to the same instance of 'TransactionServie'
                kafkaPollOptions << config.pollOptions
                kafkaInboxOptions << config.kafkaInboxOptions
            }
        } )( inbox.location )

        // Load the outbox service as an embedded service
        with (outboxSettings){
            .pollSettings << config.pollOptions;
            .databaseConnectionInfo << config.serviceAConnectionInfo;
            .brokerOptions << config.kafkaOutboxOptions;
            .transactionServiceLocation << TransactionService.location
        }

        connectKafka@OutboxService(outboxSettings)
        
        
        // Connect the TransactionService to the database 
        connect@TransactionService( config.serviceAConnectionInfo )( void )

        scope ( createTable )  // Create the table containing the 'state' of service A
        {   
            install ( SQLException => println@Console("SQLException while creating 'numbers' table in Service A")() )
            
            // Start a transaction, and get the associated transaction handle
            initializeTransaction@TransactionService()( tHandle )

            with ( createTableRequest ){
                .update = "CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, " +
                "number int)";
                .handle = tHandle
            }
            '
            // Create the table 
            executeUpdate@TransactionService( createTableRequest )( createTableResponse )
            commit@TransactionService( tHandle )( )
        }
    }

    //TODO: A third service should really be running, looking for updates to the inbox table and
    //      forwarding these to the 'main' service. This would cause no messages to be skipped

    main {
        [ updateNumber( req )( res )
        {
            println@Console("ServiceA: \tUpdateNumber called with username " + req.username)()
            scope ( UpdateLocalState )    //Update the local state of Service A
            {   
                install ( SQLException => println@Console( "SQL exception occured in Service A while updating local state" )( ) )

                // Check if the user exists, or if it needs to be created
                with ( userExistsQuery ){
                    .query = "SELECT * FROM Numbers WHERE username = '" + req.username + "';";
                    .handle = req.handle
                }
                executeQuery@TransactionService( userExistsQuery )( userExists )
                
                updateQuery.handle = req.handle
                if (#userExists.row < 1){
                    updateQuery.update = "INSERT INTO Numbers VALUES ('" + req.username + "', 0);"
                } else{
                    updateQuery.update = "UPDATE Numbers SET number = number + 1 WHERE username = '" + req.username + "'"
                }

                executeUpdate@TransactionService( updateQuery )( updateResponse )

                if ( updateResponse > 0){       // Some row(s) were updated
                    println@Console("\n Service A has executed update: " + updateQuery.update + "\n")()
                } else {    // Some error might have occured.
                    println@Console("No rows were update in Service A!")()
                }
                
                with ( outboxQuery ){
                    .tHandle = req.handle;
                    .commitTransaction = true;
                    .topic = config.kafkaOutboxOptions.topic;
                    .key = "updateNumber";
                    .value = req.username
                }

                updateOutbox@OutboxService( outboxQuery )( updateResponse )
                res = "Choreography Started!"
            }
        }]

        [finalizeChoreography( req )]{
            commit@TransactionService( req.handle )( result )
            println@Console("Finished choreography")()
        }
    }
}