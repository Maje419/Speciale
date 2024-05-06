type ReactRequest{
    .username: string
    .txHandle: long
}

interface ServiceCInterface{
    RequestResponse:
        react( ReactRequest )( bool )
}