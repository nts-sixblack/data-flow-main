#!/bin/bash

# build docker image with image name hadoop-base:3.3.6
docker build -t hadoop-base:3.3.6 -f Dockerfile .
