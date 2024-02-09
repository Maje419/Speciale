package jolie.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

//  Kafka imports
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaConsumerService extends JavaService {
    private Object lock = new Object();
    private KafkaConsumer<String, String> consumer;
    private String topic;

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
        Value kafkaOptions = input.getFirstChild("brokerOptions");
        Value pollOptions = input.getFirstChild("pollOptions");

        props.setProperty("bootstrap.servers", kafkaOptions.getFirstChild("bootstrapServer").strValue());
        props.setProperty("group.id", kafkaOptions.getFirstChild("groupId").strValue());
        props.setProperty("max.poll.records", pollOptions.getFirstChild("pollAmount").strValue());

        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        topic = kafkaOptions.getFirstChild("topic").strValue();

        synchronized (lock) {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
        }

        response.getFirstChild("topic").setValue(topic);
        response.getFirstChild("bootstrapServer").setValue(props.getProperty("bootstrap.servers"));
        response.getFirstChild("groupId").setValue(props.getProperty("group.id"));
        return response;
    }

    /**
     * @param input
     *              .timeoutMs: long - How long the consumer should wait for a
     *              response form Kafka
     */
    public Value consume(Value input) {
        long timeout = input.getFirstChild("timeoutMs").longValue();
        ConsumerRecords<String, String> records = null;

        Value response = Value.create();

        // We need to do this stupid stuff, because the consumer otherwise uses its
        // locally stored
        // offset when polling for new messages, and this could cause some message to
        // not get handled at
        // allJavaServices/kafka-consumer/kafka-consumer/target/kafka-consumer-1.0-SNAPSHOT.jar
        // if it's lost after leaving this consumer service.
        try {
            Set<TopicPartition> pars = consumer.assignment();
            long lastCommittedOffset = consumer.committed(pars).values().iterator().next().offset();
            consumer.seek(pars.iterator().next(), lastCommittedOffset);
        } catch (NoSuchElementException | NullPointerException e) {
            // The consumer might not be initialized. It'll come, let it be.
        }

        synchronized (lock) {
            if (consumer != null) {
                response.getFirstChild("status").setValue(0);
                records = consumer.poll(Duration.ofMillis(timeout));
            } else {
                System.out.println("Consumer not initialized!");
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
                }
            }
        }

        return response;
    }

    /**
     * 
     * @param input
     *              .offset: long - The offset of the last message which has been
     *              confirmed to be delivered
     */

    public Value commit(Value input) {
        System.out.println("\n\n COMMIT CALLED \n\n");
        Value response = Value.create();
        long offset = input.getFirstChild("offset").longValue() + 1; // +1 since we're committing what the offset of the
                                                                     // NEXT message
        try {
            synchronized (lock) {
                consumer.commitSync();
            }
            response.getFirstChild("reason").setValue("Committed offset " + offset + " for topic " + topic);
            response.getFirstChild("status").setValue(1);

        } catch (
                CommitFailedException | RebalanceInProgressException ex) {
            response.getFirstChild("reason").setValue(ex.getMessage());
            response.getFirstChild("status").setValue(0);
        }

        return response;
    }
}
