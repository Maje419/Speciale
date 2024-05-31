**KafkaTestTool:** A utility tool used by the tests in the Subdirectory `Safe`. This tool provides the operations `readSingle` and `send`, which can be used to read a single message from Kafka, or send a single message into kafka. This tool can thus be used to test Service A and Service B seperatly.

**Safe:** Contains tests of Service A and Service B where these implement the Outbox and Inbox parts of the Inbox/Outbox library respectively.

**Unsafe:** Contains tests of Service A and Service B where these do not implement the Inbox/Outbox pattern