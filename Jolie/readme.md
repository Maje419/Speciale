Make sure the new version of DatabaseService is included in your Jolie install. To do this, follow the guide for 'compiling from source' here: https://www.jolie-lang.org/downloads.html - Remember to clone the Maje419/jolie repository inststead in the first step.
To run the tests, take the following steps:
1. Run the docker-compose with the command 'docker-compose up' from this directory
2. From a second and third terminal window, run one instance of the producer and one of the consumers by the command 'jolie SafeProducer/serviceA.ol' and 'jolie SafeConsumer/serviceB.ol'. Exchange 'Safe' with 'Unsafe' to run version without Inbox/Outbox library.
3. From a fourth terminal window, run the python script 'results/record-data.py'.
4. The measurements will now be written to the file opened in the 'record-data.py' python script.
