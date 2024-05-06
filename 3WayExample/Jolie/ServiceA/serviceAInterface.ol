type StartChoreographyRequest {
    .username: string
    .txHandle*: long
}

type FinalizeChoreographyRequest {
    .username: string
    .txHandle*: long
}

type StartChoreographyResponse: string


interface ServiceAInterfaceExternal{
    RequestResponse:
        startChoreography( StartChoreographyRequest )( StartChoreographyResponse )
}

interface ServiceAInterfaceLocal{
    OneWay:
        finalizeChoreography( FinalizeChoreographyRequest )
}