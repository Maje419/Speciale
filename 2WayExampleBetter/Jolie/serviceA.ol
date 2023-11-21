from database import Database
from console import Console
from file import File
from runtime import Runtime

from .serviceAInterface import ServiceAInterface
from .Outbox.outboxService import OutboxInterface
from .TransactionService.transactionService import TransactionService

service ServiceA{
    execution: concurrent

    // We only need to receive on local, since Inbox will handle external messages
    inputPort ServiceALocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    // We need to be able to send messages to the Outbox Service to tell it what to put in the outbox
    outputPort OutboxService {
        Location: "local"
        protocol: http{
            format = "json"
        }
        Interfaces: OutboxInterface
    }
    embed File as File
    embed Console as Console
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
        } )( )

        // Load the outbox service as an embedded service
        loadEmbeddedService@Runtime( {
            filepath = "Outbox/outboxService.ol"
            params << { 
                pollSettings << config.pollOptions;
                databaseConnectionInfo << config.serviceAConnectionInfo;
                brokerOptions << config.kafkaOutboxOptions;
                transactionServiceLocation << TransactionService.location
            }
        } )( OutboxService.location )    // It is very important that this is a lower-case 'location', otherwise it doesn't work
                                        // Guess how long it took me to figure that out :)
        
        
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

                println@Console("1")()
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

                println@Console("2")()
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