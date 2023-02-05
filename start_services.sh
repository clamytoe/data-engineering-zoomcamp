#!/usr/bin/zsh

cd /home/clamytoe/Projects/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql
docker-compose up -d

prefect orion start
