This directory contains a choreography of 3-way communication between services A, B and C, and is used to demonstrate the use of the Inbox/Outbox pattern to make
the choreography guarentee at-least-once delivery of all messages.

Make sure you are running the newest version of the Database service by compiling Jolie from source following the guide here: https://www.jolie-lang.org/downloads.html.
Then follow these steps to run tests:
1. Navigate to this directory and run command 'npm install'
2. Run command 'npm run start' to start the necessary docker container and both services. Wait for the prompt to enter a username.
3. Write any username, and see the choreography in action
4. To stop and remove the docker containers after exectuion, the script 'remove-docker.sh' is provided