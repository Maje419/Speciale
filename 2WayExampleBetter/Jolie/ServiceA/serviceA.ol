from database import Database
from console import Console
from file import File
from runtime import Runtime
from json-utils import JsonUtils

from .serviceAInterface import ServiceAInterfaceLocal
from ..InboxOutbox.publicOutboxTypes import OutboxInterface

service ServiceA{
    execution: concurrent

    // We only need to receive on local, since Inbox will handle external messages
    inputPort ServiceALocal {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterfaceLocal
    }

    // Port for sending messages to the IBOB service
    outputPort IBOB {
        Location: "local"
        protocol: http{
            format = "json"
        }
        Interfaces: OutboxInterface
    }

    embed Console as Console
    embed Database as Database
    embed File as File
    embed JsonUtils as JsonUtils
    embed Runtime as Runtime

    init
    {
        with ( connectionInfo ){
            .username = "postgres"
            .password = "example"
            .database = "service-a-db"
            .driver = "postgresql"
            .host = ""
        }

        with ( kafkaInboxOptions ){
            .bootstrapServer = "localhost:29092"
            .groupId = "service-a-inbox"
            .topic = "b-out"
            .pollAmount = 3
        }

        with ( kafkaOutboxOptions ){
                .topic = "a-out"
                .bootstrapServer = "localhost:29092"
        }

        // Connect to the database service initiates connectionpool
        connect@Database( connectionInfo )( void )
        update@Database("CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, " +
            "number int)")()

        // Store the location of this service in a variable
        getLocalLocation@Runtime()(location)
        
        // Configuration parameters needed for the inbox service(s)
        with( inboxConfig )
        { 
            .pollTimer = 10;
            .locations << {
                .localLocation << location;
                .databaseServiceLocation << Database.location
            }
            .kafkaOptions << kafkaInboxOptions
        }

        // Configuration parameters needed for the outbox service(s)
        with ( outboxConfig ){
            .pollTimer = 10;
            .brokerOptions << kafkaOutboxOptions;
            .databaseServiceLocation << Database.location
        }

        // Load the InboxOutbox library as an embedded service
        loadEmbeddedService@Runtime({
            filepath = "InboxOutbox/ibob.ol"
            params << { 
                .inboxConfig << inboxConfig;
                .outboxConfig << outboxConfig
                }
        })(IBOB.location)

        // Load the self-written inboxServiceA to store incomming messages
        loadEmbeddedService@Runtime({
            filepath = "ServiceA/inboxServiceA.ol"
            params << IBOB.location
        })()
    }

    main {
        [ startChoreography( req )( res )
        {
            println@Console("Recieved message for operation startChoreography!")()
            // If the handle is not defined, the message was recieved from a Jolie service which is not the InboxReader
            //  We can therefore initiate a new transaction here, and be sure that we're not duplicating an update.
            // Used only in testing.
            if (!is_defined(req.txHandle)){
                beginTx@Database()(req.txHandle)
            }

            scope ( UpdateLocalState )    //Update the local state of Service A
            {
                // Check if the user exists, or if it needs to be created
                with ( userExistsQuery ){
                    .query = "SELECT * FROM Numbers WHERE username = '" + req.username + "';";
                    .txHandle = req.txHandle
                }
                query@Database( userExistsQuery )( userExists )
                
                // Create user if needed, otherwise update the number for the user
                updateQuery.txHandle = req.txHandle
                if (#userExists.row < 1){
                    updateQuery.update = "INSERT INTO Numbers VALUES ('" + req.username + "', 0);"
                } else{
                    updateQuery.update = "UPDATE Numbers SET number = number + 1 WHERE username = '" + req.username + "'"
                }
                update@Database( updateQuery )( updateResponse )

                // To update Service B, this service needs to communicate to its outbox that it should put a message
                // into kafka containing the operation and the request that we seek to call at                
                with ( outboxQuery ){
                    .txHandle = req.txHandle;                           // The transaction handle for the ongoing transaction
                    .topic = "a-out";                                   // .topic = "a-out", which is where service B inbox listens
                    .operation = "react"                                // This service wants to call serviceB.react
                }

                // Parse the request that was supposed to be sent to serviceB into a json string, and enter it into the kafka message.value
                getJsonString@JsonUtils( {.username = req.username} )(outboxQuery.parameters)
                
                updateOutbox@IBOB( outboxQuery )( updateResponse )
                
                res = "Choreography Started: " + updateResponse.success
            }
        }]

        [finalizeChoreography( req )]{
            println@Console("Finished choreography for user " + req.username)()
        }
    }
}