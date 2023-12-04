interface  SimpleKafkaConnectorInterface {
    OneWay: propagateMessage ( string )
}

service SimpleKafkaConnector{
    inputPort Input {
        Location: "local"
        Interfaces: SimpleKafkaConnectorInterface
        } 
        foreign java {
            class: "example.KafkaRelayer"
        }
}