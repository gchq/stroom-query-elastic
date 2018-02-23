#!/bin/bash

#exit script on any error
set -e

#TODO: Update this build file to support CRON jobs.
./gradlew -Pversion=$TRAVIS_TAG clean build shadowJar

if [ -n "$TRAVIS_TAG" ]; then
    echo "Travis Tag is Set, Sending to Docker"

    # Conditionally tag git and build docker image.
    # The username and password are configured in the travis gui
    docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"

    QUERY_ELASTIC_SERVICE_TAG="gchq/stroom-query-elastic-service:${TRAVIS_TAG}"
    echo "Building stroom-query-elastic-service with tag ${QUERY_ELASTIC_SERVICE_TAG}"
    docker build --tag=${QUERY_ELASTIC_SERVICE_TAG} stroom-query-elastic-svc/.
    echo "Pushing ${QUERY_ELASTIC_SERVICE_TAG}"
    docker push ${QUERY_ELASTIC_SERVICE_TAG}

    QUERY_ELASTIC_UI_TAG="gchq/stroom-query-elastic-ui:${TRAVIS_TAG}"
    echo "Building stroom-query-elastic-ui with tag ${QUERY_ELASTIC_UI_TAG}"
    ./stroom-query-elastic-ui/docker/build.sh ${TRAVIS_TAG}.
    echo "Pushing ${QUERY_ELASTIC_UI_TAG}"
    docker push ${QUERY_ELASTIC_UI_TAG}
else
    echo "Skipping Docker Push - No Tag Set"
fi