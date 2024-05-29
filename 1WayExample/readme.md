Make sure the new version of DatabaseService is included in your Jolie install. To do this, follow the guide for 'compiling from source' here: https://www.jolie-lang.org/downloads.html - Remember to clone the Maje419/jolie repository inststead in the first step.
1. Run the command 'npm install' to install necessary packages
2. Run the command 'npm run record' to start all needed docker containers as well as Service A and Service B - (npm run run-services to run services w.o. writing timings to file)
3. To initiate the choreography once, send a curl request to endpoint 'http://localhost:8080/startChoreography?username=user1'
4. To run all measurements, run the python script 'results/record-data.py'. The data will be recorded to the 'results/tempResults.csv' file if the record command was used.
