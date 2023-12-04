type UpdateDatabaseRequest{
    .userToUpdate: string
}

type UpdateDatabaseResponse {
    .code: int
    .reason: string
}

interface ServiceBInterface{
    RequestResponse:
        updateNumberForUser( UpdateDatabaseRequest )( UpdateDatabaseResponse )
}
