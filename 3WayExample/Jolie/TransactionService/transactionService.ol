from database import ConnectionInfo

type TransactionHandle: string
type TransactionResult: string

type ConnectRequest: ConnectionInfo
type ConnectResponse: string

type QueryRequest{
    .handle: TransactionHandle
    .query: string
}
type QueryResult: void {
    .row*: undefined
}

type UpdateRequest{
    .handle: TransactionHandle
    .update: string
}

type UpdateResponse: int
type AbortResponse: bool


interface TransactionServiceSetup {
    RequestResponse: 
        connect( ConnectRequest ) ( ConnectResponse )
}

interface TransactionServiceOperations {
    RequestResponse:
        initializeTransaction( void )( TransactionHandle ),
        executeQuery( QueryRequest )( QueryResult ),
        executeUpdate( UpdateRequest )( UpdateResponse ),
        commit( TransactionHandle )( TransactionResult ),
        abortTransaction( TransactionHandle )( AbortResponse )
}

service TransactionService{
    inputPort Input {
        Location: "local"
        Interfaces: TransactionServiceSetup, TransactionServiceOperations
        } 
        foreign java {
            class: "jolie.transactionservice.TransactionService"
        }
}