#!/bin/bash
docker stop $(docker ps -a -q)
docker container prune -f
docker volume prune -f
docker volume rm 3wayexample_pgdata
docker volume rm jolie_pgdata
docker volume rm docker_pgdata
