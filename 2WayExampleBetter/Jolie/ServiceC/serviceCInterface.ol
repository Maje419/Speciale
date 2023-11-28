type UpdateNumbersRequest{
    .username: string
    .handle: string
}

interface ServiceCInterface{
    RequestResponse:
        updateNumbers( UpdateNumbersRequest )( bool )
}