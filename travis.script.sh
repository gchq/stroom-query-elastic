#!/bin/bash
#TODO: Update this build file to support CRON jobs.
./gradlew -Pversion=$TRAVIS_TAG clean build shadowJar

if [ -n "$TRAVIS_TAG" ]; then
    echo "Travis Tag is Set, Sending to Docker"

    # Conditionally tag git and build docker image.
    # The username and password are configured in the travis gui
    docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"

    ELASTIC_TAG="gchq/stroom-query-elastic:${TRAVIS_TAG}"
    echo "Building stroom-query-elastic with tag ${ELASTIC_TAG}"
    docker build --tag=${ELASTIC_TAG} .
    echo "Pushing ${ELASTIC_TAG}"
    docker push ${ELASTIC_TAG}
else
    echo "Skipping Docker Push - No Tag Set"
fi