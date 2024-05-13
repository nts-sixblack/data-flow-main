#!/bin/bash

docker start superset_worker_beat
docker start superset_worker
docker start superset_init
docker start superset_app
docker start superset_db
docker start superset_cache
