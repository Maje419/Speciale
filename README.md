This repository contains the final product(s) of my thesis on Programming Microservice Choreographies.

The repository contains 4 directories:
1. **1WayExample**: A directory containing an example of a 1-way choreography from Service A to Service B. This choreography is used to record the delay introduced by the Inbox/Outbox library compared to not using it.
2. **2WayExample**: This directory contains an example choreography where 2 services are conversing through Kafka, both services using the Inbox and the Outbox patterns implemented by the Inbox/Outbox library
3. **3WayExample**: The same as above, but this time the choreography includes 3 services
4. **Tests**: This directory contains tests which are made to confirm that the Inbox/Outbox library indeed guarentees at-least-once delivery of messages if the message caused a state-update in the service, as well as tests for the new version of the Database Service which was implemented as part of the thesis.

The final version of the Inbox/Outbox library is located in both directories **2WayExample/Jolie** and **3WayExample/Jolie**
