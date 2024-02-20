type UpdateNumberRequest {
    .username : string
    .handle[0, 1] : string
}

type FinalizeChoreographyRequest {
    .username: string
    .handle[0,1]: string
}

type UpdateNumberResponse: string


interface ServiceAInterfaceExternal{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse ) throws TestException
}

interface ServiceAInterfaceLocal{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse ) throws TestException
}

interface ServiceAInterfaceLocal{
    OneWay:
        finalizeChoreography( FinalizeChoreographyRequest )
}