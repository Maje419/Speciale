type UpdateNumberRequest {
    .username : string
    .handle[0, 1] : string
}

type FinalizeChoreographyRequest {
    .username: string
    .handle[0,1]: string
}

type UpdateNumberResponse: string


interface ServiceAInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse )
    OneWay:
        finalizeChoreography( FinalizeChoreographyRequest )
}