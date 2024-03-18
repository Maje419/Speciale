type NumbersUpdatedRequest{
    .username: string
    .handle[0, 1]: string   // [0, 1] here is not needed, but a good indication that this has been added as an extension to the normal request
}

interface ServiceBInterface {
    RequestResponse:
        numbersUpdated( NumbersUpdatedRequest ) ( void )
}
