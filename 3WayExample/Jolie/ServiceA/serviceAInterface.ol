type UpdateNumberRequest {
    .username: string
    .handle*: string
}

type FinalizeChoreographyRequest {
    .username: string
    .handle: string
}

type UpdateNumberResponse: string


interface ServiceAInterfaceExternal{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse )
}

interface ServiceAInterfaceLocal{
    OneWay:
        finalizeChoreography( FinalizeChoreographyRequest )
}