Tests showing that Service A and Service B can become inconsistant through crashes when not implementing the Inbox/Outbox pattern. These tests may be flakey due to their reliance on both Service A and Service B interacting within a given timeframe.

Make sure you are running the newest version of the Database service by compiling Jolie from source following the guide here: https://www.jolie-lang.org/downloads.html.
+ If any issues arise when running script/setup-dev.sh saying 'No such file or directory', my workaround is to run the command `scripts/dev-setup.sh /bin/` instead, and then export `/bin/jolie-dist` to the `$JOLIE_HOME` variable instead.

1. Run `npm install`
2. Run `docker-compose up`
2. Run `npm run testProducer` or `npm run testConsumer` to test Service A and Service B respectively