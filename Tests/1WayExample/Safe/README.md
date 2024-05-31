Tests confiming that when using the Inbox/Outbox library, Service A and Service B will always stay consistant through crashes.

Make sure you are running the newest version of the Database service by compiling Jolie from source following the guide here: https://www.jolie-lang.org/downloads.html.
+ If any issues arise when running script/setup-dev.sh saying 'No such file or directory', my workaround is to run the command `scripts/dev-setup.sh /bin/` instead, and then export `/bin/jolie-dist` to the `$JOLIE_HOME` variable instead.

1. Run `npm install`
2. Run `docker-compose up`
2. Run `npm run testProducer` or `npm run testConsumer` to test Service A and Service B respectively