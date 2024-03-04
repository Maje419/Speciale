from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

from .serviceAInterface import ServiceAInterfaceExternal, ServiceAInterfaceLocal
from ..Outbox.outboxService import OutboxInterface
from ..TransactionService.transactionService import TransactionService
from ...test.testTypes import TestInterface

service ServiceA{
    execution: concurrent

    // We only need to receive on local, since Inbox will handle external messages
    inputPort ServiceALocal {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterfaceLocal, ServiceAInterfaceExternal, TestInterface
    }

    // We need to be able to send messages to the Outbox Service to tell it what to put in the outbox
    outputPort OutboxService {
        Location: "local"
        protocol: http{
            format = "json"
        }
        Interfaces: OutboxInterface, TestInterface
    }

    outputPort InboxService {
        Location: "local"
        protocol: http{
            format = "json"
        }
        Interfaces: TestInterface
    }

    embed File as File
    embed Console as Console
    embed JsonUtils as JsonUtils
    embed TransactionService as TransactionService
    embed Runtime as Runtime

    init
    {
        // serviceAConfig.json contains info for connecting to db, as well as kafka settings
        readFile@File(
            {
                filename = "Jolie/ServiceA/serviceAConfig.json"
                format = "json"
            }) ( config )

        getLocalLocation@Runtime()(location)

        with( inboxConfig )
        { 
            .localLocation << location;
            .externalLocation << "socket://localhost:8080";       //This doesn't work (yet)
            .databaseConnectionInfo << config.serviceAConnectionInfo;
            .transactionServiceLocation << TransactionService.location;   // All embedded services must talk to the same instance of 'TransactionServie'
            .kafkaPollOptions << config.pollOptions;
            .kafkaInboxOptions << config.kafkaInboxOptions
        }

        loadEmbeddedService@Runtime({
            filepath = "Jolie/ServiceA/inboxServiceA.ol"
            params << inboxConfig
        })(InboxService.location)

        // Load the outbox service as an embedded service
        loadEmbeddedService@Runtime( {
            filepath = "Jolie/Outbox/outboxService.ol"
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
            
            // Start a transaction, and get the associated transaction handle
            initializeTransaction@TransactionService()( tHandle )

            with ( createTableRequest ){
                .update = "CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, " +
                "number int)";
                .handle = tHandle
            }
            
            // Create the table 
            executeUpdate@TransactionService( createTableRequest )( createTableResponse )
            commit@TransactionService( tHandle )( )
        }
    }

    main {
        [ updateNumber( req )( res )
        {
            if (global.testParams.throw_on_updateNumber_called){
                throw ( TestException, "throw_on_updateNumber_called" )
            }

            // If the handle is not defined, the message was recieved from a Jolie service which is not the InboxReader
            //  We can therefore initiate a new transaction here, and be sure that we're not duplicating an update
            if (!is_defined(req.handle)){
                initializeTransaction@TransactionService()(req.handle)
            }

            scope ( UpdateLocalState )    //Update the local state of Service A
            {
                // Check if the user exists, or if it needs to be created
                with ( userExistsQuery ){
                    .query = "SELECT * FROM Numbers WHERE username = '" + req.username + "';";
                    .handle = req.handle
                }
                executeQuery@TransactionService( userExistsQuery )( userExists )
                
                // Create user if needed, otherwise update the number for the user
                updateQuery.handle = req.handle
                if (#userExists.row < 1){
                    updateQuery.update = "INSERT INTO Numbers VALUES ('" + req.username + "', 0);"
                } else{
                    updateQuery.update = "UPDATE Numbers SET number = number + 1 WHERE username = '" + req.username + "'"
                }
                executeUpdate@TransactionService( updateQuery )( updateResponse )

                if (global.testParams.throw_after_local_updates_executed){
                    throw ( TestException, "throw_after_local_updates_executed" )
                }

                // To update Service B, this service needs to communicate to its outbox that it should put a message
                // into kafka containing the operation and the request that we seek to call at                
                with ( outboxQuery ){
                    .tHandle = req.handle;                          // The transaction handle for the ongoing transaction
                    .commitTransaction = true;                      // No other updates should come to this transaction, so commit it.
                    .topic = config.kafkaOutboxOptions.topic;       // .topic = "a-out", which is where service B inbox listens
                    .operation = "numbersUpdated"                   // This service wants to call serviceB.numbersUpdated
                }

                // Parse the request that was supposed to be sent to serviceB into a json string, and enter it into the kafak message.value
                getJsonString@JsonUtils( {.username = req.username} )(outboxQuery.data)
                
                scope (HandleOutboxThrow){
                    install(TestException => {  // I have to install this here, otherwise the tests won't work bcause of the throw
                        println@Console("updateOutbox threw an exception!")()
                    })
                    updateOutbox@OutboxService( outboxQuery )( updateResponse )
                }
                res = "Choreography Started: " + updateResponse.success
            }
        }]

        [finalizeChoreography( req )]{
            if (global.testParams.throw_on_finalizeChoreography_called){
                throw ( TestException, "throw_on_finalizeChoreography_called" )
            }

            commit@TransactionService( req.handle )( result )

            if (global.testParams.throw_after_transaction_committed){
                throw ( TestException, "throw_after_transaction_committed" )
            }

            println@Console("Finished choreography for user " + req.username)()
        }

        [ setupTest( req )( res ){
            global.testParams << req.serviceA
            setupTest@OutboxService(req)(resOutbox)
            println@Console("ResOutbox: " + resOutbox)()
            setupTest@InboxService(req)(resInbox)

            res = (resOutbox && resInbox)
        }]
    }
}