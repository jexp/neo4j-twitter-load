#!/bin/bash

cd $(dirname $0)

mvn clean install
docker build --tag jexp/neo4j-twitter-load:latest .

# docker push jexp/neo4j-twitter-load:latest
