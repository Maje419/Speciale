package jolie.kafka.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

//  Kafka imports
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaConsumerService extends JavaService {
    private Object lock = new Object();
    private KafkaConsumer<String, String> consumer;

    public void Initialize(Value input) {
        CountDownLatch latch = new CountDownLatch(1);

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("group.id", "example-group");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        synchronized (lock) {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singleton("example"),
                    new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            System.out.println("Consumer partitions revoked");
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            latch.countDown();
                        }
                    }); // TODO: Check if it makes sense to use assign here instead
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            // Do nothing
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

    /**
     * kafkaTestObject.setupConsumer( {topic: string} ) => Sets up new consumer
     * which is subscribed to 'topic', allowing for monitoring messages
     * kafkaTestObject.setupProducer( {topig: string} ) => Sets up a new producer
     * which can be used to input messages into kafka
     */
}