This directory contains a choreography of 1-way communication between services A and B, and is used to gather data on how much delay is introduced when using the Inbox/Outbox library.

Make sure you are running the newest version of the Database service by compiling Jolie from source following the guide here: https://www.jolie-lang.org/downloads.html.
Then follow these steps to run tests:
1. Navigate to this directory
2. Run the command 'npm install' to install necessary packages
3. Run the command 'npm run record-safe' to start all needed docker containers as well as Service A and Service B - (npm run run-unsafe to run version of Service A and B with no Inbox/Outbox)
4. To initiate the choreography once, send a curl request to endpoint 'http://localhost:8080/startChoreography?username=user1'
5. To run all measurements, run the python script 'results/record-data.py'. The data will be recorded to the 'results/tempResults.csv' file.
6. To stop and remove the docker containers after exectuion, the script 'remove-docker.sh' is provided

