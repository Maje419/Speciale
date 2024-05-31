This directory contains a choreography of 2-way communication between services A and B, and is used to demonstrate the use of the Inbox/Outbox pattern to make
the choreography guarentee at-least-once delivery of all messages.

Make sure you are running the newest version of the Database service by compiling Jolie from source following the guide here: https://www.jolie-lang.org/downloads.html.
+ If any issues arise when running script/setup-dev.sh saying 'No such file or directory', my workaround is to run the command `scripts/dev-setup.sh /bin/` instead, and then export `/bin/jolie-dist` to the `$JOLIE_HOME` variable instead.

Then follow these steps to run tests:
1. Navigate to this directory and run command `npm install`
2. Run command `npm run start` to start the necessary docker container and both services. Wait for the prompt to enter a username.
3. Write any username, and see the choreography in action
4. Advanced users can use `docker exec -it jolie-db-1 bash` to check the database continaer, and then `psql -U postgres` to interact with Postgres, to confirm that the databases `service-a-db` and `service-b-db` are both updated.
5. To stop and remove the docker containers after exectuion, the script `remove-docker.sh` is provided