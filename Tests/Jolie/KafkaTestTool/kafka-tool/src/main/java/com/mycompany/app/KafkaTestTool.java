package com.mycompany.app;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

//  Kafka imports
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaTestTool extends JavaService {
    private KafkaConsumer<String, String> testConsumer = null;
    private KafkaProducer<String, String> testProducer = null;

    /**
     * Sets up a consumer and subscribes it to 'topic' in the bootstrapServer Kafka.
     * Calling this method will trigger a rebalancing of consumers in 'topic', and
     * will only return
     * after that rebalancing is finished.
     * 
     * @param input - A Jolie value with the children:
     *              .bootstrapServer - The server running kafka
     *              .topic - The topic that the consumers should be listening on
     * 
     * @return - A Jolie value with the following children:
     *         .status - "Ready" | "Interrupted"
     */
    // Create a new consumer which will respond only once the tests are balanced
    public Value setupTestConsumer(Value input) {
        Value response = Value.create();
        Properties props = new Properties();

        // Pass in the bootstrap server, as this might change in some imaginable
        // scenario
        // props.setProperty("bootstrap.servers",
        // input.getFirstChild("bootstrapServer").strValue());
        props.setProperty("bootstrap.servers", "localhost:9092");

        // Giving this a seperate groupId from the actual services ensures that all
        // messages are still read by the original service
        props.setProperty("group.id", "testConsumer");

        // MaxPollRecords set to 1, as to have the consumer pull 1 record per poll. This
        // is probably inconsequential
        props.setProperty("max.poll.records", "1");

        // We will like to manually control committing, as it is needed to call "poll"
        // to 'prime' the consumer and Kafka to actually realize that it's been assigned
        // some partition
        props.setProperty("enable.auto.commit", "false");

        // Use the default deserializer.
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Since this consumer needs to know whether a rebalance is in progress, we use
        // 'subscribe' and not 'assign'
        testConsumer = new KafkaConsumer<>(props);
        testConsumer.subscribe(Collections.singleton(input.getFirstChild("topic").strValue()));

        /*
         * Note that here, I'd have created a much smarter way of counting when the
         * consumer was connected, namely using a ConsumerRebalanceListener:
         * 
         * consumer.subscribe(Collections.singleton("example"),
         * new ConsumerRebalanceListener() {
         * 
         * @Override
         * public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
         * System.out.println("Consumer partitions revoked");
         * }
         * 
         * @Override
         * public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
         * System.out.println("Consumer partitions revoked");
         * latch.countDown();
         * }
         * });
         * 
         * BUT
         * This did not work, since the callback onPartitionsAssigned was only called
         * when exiting this method, and I could therefore not latch.await().
         * 
         * Hence the stupid dumb way I'm doing below, listing the topics until something
         * comes up.
         */
        try {
            while (testConsumer.assignment().size() <= 0) {
                // I HAVE TO DO THIS DUMB SHOT FOR KAFKA TO REALIZE THAT THE CONSUMER HAS A
                // PARTITION ASSIGNED vvv
                testConsumer.poll(Duration.ofMillis(50));
                Thread.sleep(1000);
            }
            // SeekToEnd to only read new messages for each topic
            testConsumer.seekToEnd(testConsumer.assignment());
            response.getFirstChild("status").setValue(("Ready"));
            return response;
        } catch (InterruptedException e) {
            response.getFirstChild("status").setValue(("Error"));
            return response;
        }
    }

    /**
     * Reads a single value from Kafka using the testConsumer. Will block until a
     * message is found, but at most 5 seconds
     * 
     * @return - A Jolie value with the following children:
     *         .status - "Consumer not loaded" | "No records found" | "Recieved"
     *         .message - The received message
     */
    public Value readSingle() {
        Value response = Value.create();

        if (testConsumer == null) {
            response.getFirstChild("status").setValue("Consumer not loaded");
            response.getFirstChild("message").setValue("null");
            return response;
        }

        ConsumerRecords<String, String> records = new ConsumerRecords<>(null);

        // Block for 5 seconds before returning that no records were found
        records = testConsumer.poll(Duration.ofSeconds(10));
        testConsumer.commitSync();

        if (records.isEmpty()) {
            response.getFirstChild("status").setValue("No records found");
            response.getFirstChild("message").setValue("null");
        } else { // A record was found
            response.getFirstChild("status").setValue("Recieved");

            // Get the first (and only) record that was found
            // ^^ Defined by max.poll.records = 1
            ConsumerRecord<String, String> record = records.iterator().next();
            response.getFirstChild("message")
                    .setValue(String.format("Offset: %d, Key = %s, Value = %s%n", record.offset(), record.key(),
                            record.value()));
        }

        return response;
    }

    /**
     * This method sets up the producer which allowes for sending messages to
     * 'topic' in the 'bootstrapServer' kafka
     * 
     * @param input - A Jolie value with the following children:
     *              .bootstrapServicer - The bootstrap server for kafka.
     * 
     * @return - A Jolie value with the following children:
     *         .status - "Ready" | "Interrupteed"
     */
    public Value setupTestProducer(Value input) {
        Properties props = new Properties();

        props.put("bootstrap.servers", input.getFirstChild("bootstrapServer").strValue());
        props.put("group.id", "testProducer");

        // For now, these values are non-configurable
        props.put("enable.auto.commit", "true");
        props.put("client.id", "testclient");
        props.put("message.send.max.retries", "2");
        props.put("queue.buffering.max.ms", "100");
        props.put("auto.commit.interval.ms", "500");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        testProducer = new KafkaProducer<>(props);

        Value response = Value.create();
        response.getFirstChild("status").setValue("Ready");
        return response;
    }

    /**
     * Sends the message contained within 'input' into Kafka using testProducer
     * 
     * @param input - A Jolie value with the following children:
     *              .topic - The topic to send the message to
     *              .key - The key to associate the message with
     *              .value - The value contained within the message
     * 
     * @return - A Jolie value with the following children:
     *         .status - "Delivered" | Exception message
     */
    public Value send(Value input) {
        final Value response = Value.create();

        // This message must be called after setupTestProducer
        if (testProducer == null) {
            response.getFirstChild("status").setValue("Producer not setup");
            return response;
        }

        // Retrieve values from input and construct a Kafka message (ProducerRecord)
        ProducerRecord<String, String> message = new ProducerRecord<>(
                input.getFirstChild("topic").strValue(),
                input.getFirstChild("key").strValue(),
                input.getFirstChild("value").strValue());

        // Send the message into kafka
        final CountDownLatch latch = new CountDownLatch(1);

        testProducer.send(message, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    // Nothing went wrong, the message is now in Kafka
                    response.getFirstChild("status").setValue("Delivered");
                    latch.countDown();
                } else {
                    // Something went wrong, inform the caller
                    latch.countDown();
                    response.getFirstChild("status").setValue(e.getMessage());
                }
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return response;
    }
}
