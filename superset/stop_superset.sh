#!/bin/bash

docker stop superset_worker_beat
docker stop superset_worker
docker stop superset_init
docker stop superset_app
docker stop superset_db
docker stop superset_cache
