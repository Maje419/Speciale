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


interface TransactionServiceInterface {
    RequestResponse: 
        connect( ConnectRequest ) ( ConnectResponse ),
        initializeTransaction( void )( TransactionHandle ),
        executeQuery( QueryRequest )( QueryResult ),
        executeUpdate( UpdateRequest )( UpdateResponse ),
        commit( TransactionHandle )( TransactionResult )
}

service TransactionService{
    inputPort Input {
        Location: "local"
        Interfaces: TransactionServiceInterface
        } 
        foreign java {
            class: "jolie.transactionservice.TransactionService"
        }
}