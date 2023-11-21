type InboxUpdatedResponse {
    .amountMessagesRead: int
}

interface ServiceBInterface {
    OneWay:
        numbersUpdated( string )
}
