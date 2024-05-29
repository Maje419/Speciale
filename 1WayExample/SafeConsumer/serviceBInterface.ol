type UpdateDatabaseRequest{
    .username: string
    .txHandle[0,1]: long
}

type UpdateDatabaseResponse: string

interface ServiceBInterface{
    RequestResponse:
        react( UpdateDatabaseRequest )( UpdateDatabaseResponse )
}
