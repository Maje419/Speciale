# JolieDatabaseTests
Tests for the jolie database service

To run:
1. Clone this repository
2. Run 'npm install'
3. Run 'npm run testHSQL' or 'npm run timeHSQL'
4. To test with PostgreSQL instead of HSQL, first run 'docker-compose up' in a seperate terminal window, then run 'npm run testPSQL' or 'npm run timePSQL'
6. To test with the new version of the DatabaseService, follow the guide for 'compiling from source' here: https://www.jolie-lang.org/downloads.html