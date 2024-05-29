package jolie.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//  Kafka imports
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class UsafeKafkaConsumerService extends JavaService {
    private Object lock = new Object();
    private KafkaConsumer<String, String> consumer;
    private String topic;
    private int pollTimeout;

    /**
     * @param input
     *              .bootstrapServers: string - The IP of the kafka server to use
     *              for
     *              bootstrapping onto the cluster, i.e. "localhost:9092"
     *              .groupId: string - This id of the group this consumer belongs
     *              to.
     *              .topic: string - The topic this consumer should subscribe to
     *              .pollAmount: str - The number of records that can be consumed
     *              from the kafka queue per poll
     */
    public Value initialize(Value input) {
        Value response = Value.create();
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", input.getFirstChild("bootstrapServer").strValue());
        props.setProperty("group.id", input.getFirstChild("groupId").strValue());
        props.setProperty("max.poll.records", input.getFirstChild("pollAmount").strValue());

        props.setProperty("enable.auto.commit", "true");    // Set to true for the unsafe versions
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        topic = input.getFirstChild("topic").strValue();
        pollTimeout = input.getFirstChild("pollTimeout").intValue();

        synchronized (lock) {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
        }

        response.setValue((consumer != null));
        return response;
    }

    /**
     * @param input
     *              .timeoutMs: long - How long the consumer should wait for a
     *              response form Kafka
     */
    public Value consume() {
        ConsumerRecords<String, String> records = null;
        Value response = Value.create();

        synchronized (lock) {
            if (consumer != null) {
                response.getFirstChild("status").setValue(0);
                records = consumer.poll(Duration.ofMillis(pollTimeout));
            } else {
                response.getFirstChild("status").setValue(1);
            }

            if (records != null && !records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    Value message = Value.create();
                    message.getFirstChild("offset").setValue(record.offset());
                    message.getFirstChild("key").setValue(record.key());
                    message.getFirstChild("value").setValue(record.value());
                    message.getFirstChild("topic").setValue(record.topic());
                    response.getChildren("messages").add(message);
                    //ExitedKafkaTime
                    System.out.print(" " + System.currentTimeMillis());
                }
            }
        }

        return response;
    }
}