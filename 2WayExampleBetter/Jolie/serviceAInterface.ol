type UpdateNumberRequest {
    .username : string
    .handle[0, 1] : string
}

type FinalizeChoreographyRequest {
    .finishedKafkaMessageOffset: long
    .handle[0,1]: string
}

type UpdateNumberResponse: string


interface ServiceAInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse )
    OneWay:
        finalizeChoreography( FinalizeChoreographyRequest )
}