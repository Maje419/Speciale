type NumbersUpdatedRequest{
    .username: string
    .txHandle[0, 1]: long   // [0, 1] here is not needed, but a good indication that this has been added as an extension to the normal request
}

interface ServiceBInterface {
    RequestResponse:
        react( NumbersUpdatedRequest ) ( string )
}
