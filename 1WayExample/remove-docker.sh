#!/bin/bash

docker ps -aq | xargs docker stop
docker container prune -f
docker volume prune -f
docker volume rm 3wayexample_pgdata
docker volume rm jolie_pgdata
