# JolieDatabaseTests
Tests for the jolie database service

To run:
1. Clone this repository
2. Run 'npm install'
3. Run 'npm run testHSQL' or 'npm run timeHSQL'
4. To test with PostgreSQL instead of HSQL, first run 'docker-compose up' in a seperate terminal window
5. If you are not running the new version of the Jolie DatabaseService (https://github.com/Maje419/jolie), all tests related to transactions should fail.
6. To test with the new version of the DatabaseService, follow the guide for 'compiling from source' here: https://www.jolie-lang.org/downloads.html - Remember to clone the Maje419/jolie repository inststead in the first step.