#!/bin/bash

#exit script on any error
set -e

#Shell Colour constants for use in 'echo -e'
#e.g.  echo -e "My message ${GREEN}with just this text in green${NC}"
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
NC='\033[0m' # No Colour 

echo -e "TRAVIS_EVENT_TYPE:   [${GREEN}${TRAVIS_EVENT_TYPE}${NC}]"

if [ "$TRAVIS_EVENT_TYPE" = "cron" ]; then
    echo "Cron build so don't set up gradle plugins or docker containers"

else
    echo -e "JAVA_OPTS: [${GREEN}$JAVA_OPTS${NC}]"
    # Increase the size of the heap
    export JAVA_OPTS=-Xmx1024m

    # This is a temporary measure until the library is published
    echo "Clone build and publish the stroom-query library"
    mkdir -p ../git_work
    pushd ../git_work
    git clone https://github.com/gchq/stroom-query.git
    pushd stroom-query
    ./gradlew clean build publishToMavenLocal -x integrationTest
    popd
    git clone https://github.com/gchq/stroom-test-data.git
    pushd stroom-test-data
    ./gradlew clean build publishToMavenLocal
    popd
    popd

    echo "Start all the services we need to run the integration tests in stroom"
    docker-compose -f elasticsearch-mysql-test.yml up -d


fi

exit 0
