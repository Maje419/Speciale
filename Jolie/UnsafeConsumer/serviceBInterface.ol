type UpdateDatabaseRequest{
    .username: string
}

type UpdateDatabaseResponse: string

interface ServiceBInterface{
    RequestResponse:
        react( UpdateDatabaseRequest )( UpdateDatabaseResponse )
}
