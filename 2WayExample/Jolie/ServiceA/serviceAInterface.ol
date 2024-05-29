type UpdateNumberRequest {
    .username : string
    .txHandle[0, 1] : long
}

type FinalizeChoreographyRequest {
    .username: string
    .txHandle[0,1]: long
}

type UpdateNumberResponse: string


interface ServiceAInterfaceExternal{
    RequestResponse:
        startChoreography( UpdateNumberRequest )( UpdateNumberResponse )
}

interface ServiceAInterfaceLocal{
    RequestResponse:
        startChoreography( UpdateNumberRequest )( UpdateNumberResponse )
    OneWay:
        finalizeChoreography( FinalizeChoreographyRequest )
}