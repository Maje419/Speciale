package jolie.kafka.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

//  Kafka imports
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaConsumerService extends JavaService {
    private Object lock = new Object();
    private KafkaConsumer<String, String> consumer;

    public void Initialize(Value input) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("group.id", "example-group");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        synchronized (lock) {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("example"));
        }
    }

    public Value Consume(Value input) {
        ConsumerRecords<String, String> records = null;
        ArrayList<String> messages = new ArrayList<>();

        Value response = Value.create();
        synchronized (lock) {
            if (consumer != null) {
                response.getFirstChild("code").setValue(200);
                records = consumer.poll(Duration.ofMillis(500));
            } else {
                System.out.println("Consumer not initialized!");
                response.getFirstChild("code").setValue(400);
            }
        }
        if (records != null) {
            for (ConsumerRecord<String, String> record : records) {
                messages.add(String.format("Offset: %d, Key = %s, Value = %s%n", record.offset(), record.key(),
                        record.value()));
            }
        }

        for (String message : messages) {
            response.getChildren("messages").add(Value.create(message));
        }

        return response;
    }
}