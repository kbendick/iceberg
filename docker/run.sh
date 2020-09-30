#!/usr/bin/env bash

# Remove existing notebook if exists
docker kill iceberg-notebook && docker rm iceberg-notebook

# Start the notebook
echo "Starting notebook server in container iceberg-notebook"
echo "Please get the jupyter notebook server's token from there"
docker run -d -P --name iceberg-notebook iceberg/jupyter-spark-all:latest
