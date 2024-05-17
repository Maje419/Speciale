package example;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
//  Kafka imports
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaRelayer extends JavaService {

    /**
     * Propagates a message to Kafka
     */
    public Value propagateMessage(Value input) {
        Properties props = new Properties();

        // Get these values from the request. We might want the rest of the values to
        // also be configurable, but this is future work.
        props.put("bootstrap.servers",
                input.getFirstChild("brokerOptions").getFirstChild("bootstrapServer").strValue());
        props.put(
                "group.id",
                input.getFirstChild("brokerOptions").getFirstChild("groupId").strValue());
        props.put(
                "client.id",
                input.getFirstChild("brokerOptions").getFirstChild("clientId").strValue());

        // For now, these values are non-configurable
        props.put("enable.auto.commit", "true");
        props.put("client.id", "testclient001");
        props.put("auto.commit.interval.ms", "500");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Setup Kafka connection
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> message = new ProducerRecord<>(
                input.getFirstChild("topic").strValue(),
                input.getFirstChild("key").strValue(),
                input.getFirstChild("value").strValue());
        Value response = Value.create();

        // The callback is executed once a response is recieved from Kafka
        producer.send(message, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                String statusMessage;
                if (e == null) {
                    // EnteredKafkaTime
                    System.out.print(" " + System.currentTimeMillis());
                    statusMessage = "Kafka message delivered. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp();
                    response.getFirstChild("message").setValue(statusMessage);
                    response.getFirstChild("success").setValue(true);
                } else {
                    statusMessage = e.getMessage();
                    response.getFirstChild("message").setValue(statusMessage);
                    response.getFirstChild("success").setValue(false);
                }
            }
        });
        producer.close();

        return response;
    }
}
