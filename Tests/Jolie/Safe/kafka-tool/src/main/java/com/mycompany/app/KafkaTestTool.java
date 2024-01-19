package com.mycompany.app;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

//  Kafka imports
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaTestTool extends JavaService {

    private KafkaConsumer<String, String> testConsumer;
    private KafkaProducer<String, String> testProducer;

    /**
     * Sets up a consumer and subscribes it to 'topic' in the bootstrapServer Kafka.
     * Calling this method will trigger a rebalancing of consumers in 'topic', and
     * will only return
     * after that rebalancing is finished.
     * 
     * @param input - A Jolie value with the children:
     *              .bootstrapService - The server running kafka
     *              .topic - The topic that the consumers should be listening on
     * 
     * @return - A Jolie value with the following children:
     *         .status - "Ready" | "Interrupteed"
     */
    // Create a new consumer which will respond only once the tests are balanced
    public Value setupTestConsumer(Value input) {
        Value response = Value.create();
        Properties props = new Properties();

        // Pass in the bootstrap server, as this might change in some imaginable
        // scenario
        props.setProperty("bootstrap.servers", input.getFirstChild("bootstrapServer").strValue());

        // Giving this a seperate groupId from the actual services ensures that all
        // messages are still read by the original service
        props.setProperty("group.id", "testConsumer");

        // MaxPollRecords set to 1, as to have the consumer pull 1 record per poll. This
        // is probably inconsequential
        props.setProperty("max.poll.records", "1");

        // We will likely need to always commit reads from the test class, otherwise
        // tests might become flaky
        props.setProperty("enable.auto.commit", "true");

        // Use the default deserializer.
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        testConsumer = new KafkaConsumer<>(props);

        // This latch is counted down when the testConsumer is assigned a topic, meaning
        // rebalancing is finished
        final CountDownLatch latch = new CountDownLatch(1);

        // Since this consumer needs to know whether a rebalance is in progress, we use
        // 'subscribe' and not 'assign'
        testConsumer.subscribe(Collections.singleton(input.getFirstChild("topic").strValue()),
                // We use a ConsumerRebalanceListner to have stuff happen when the partions are
                // either revoked or assigned to this consumer
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        System.out.println("Consumer partitions revoked");
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        latch.countDown();
                    }
                });
        try {
            latch.await();
            // Once this point is reached, this consumer must have partitions assigned,
            // meaning the rebalancing is over
            response.getFirstChild("status").setValue(("Ready"));
        } catch (InterruptedException e) {
            testConsumer.unsubscribe();
            testConsumer = null;
            response.getFirstChild("status").setValue(("Interrupted"));
        }
        return response;
    }

    // TODO: Create a function which allowes for consuming messages from this
    // service

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
        testProducer.send(message, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    // Nothing went wrong, the message is now in Kafka
                    response.getFirstChild("status").setValue("Delivered");
                } else {
                    // Something went wrong, inform the caller
                    response.getFirstChild("status").setValue(e.getMessage());
                }
            }
        });
        return response;
    }
}
