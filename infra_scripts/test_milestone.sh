#!/bin/bash

#### CS 165 milestone test script           ####
# this script takes generated tests,
# and kicks off running a test for each,
# via another script call for a
# single test case to be run
#
#### Contact: Wilson Qin                    ####


UPTOMILE="${1:-5}"

# the number of seconds you need to wait for your server to go from shutdown
# to ready to receive queries from client.
# the default is 5 s.
#
# Note: if you see segfaults when running this test suite for the test cases which expect a fresh server restart,
# this may be because you are not waiting long enough between client tests.
# Once you know how long your server takes, you can cut this time down - and provide it on your cmdline
WAIT_SECONDS_TO_RECOVER_DATA="${2:-7}"

# Set base directory based on the location of this script, to all this script to work in both local and container
# environments. As long as this script is in `infra_scripts` directory, this will point to the project root directory.
BASE_DIR=$(dirname $(dirname $(realpath $0)))

INPUT_DIR=
OUTPUT_DIR=


# For M1 Experiment we add support for running tests for different data sizes in `EXPERIMENT/data/milestone1/n`
RUN_M1_EXPERIMENT="${3:-0}"
DATASIZE="${4:-0}"

M1_EXPERIMENT_BASE_DIR="${BASE_DIR}/experiments/data/milestone1"

if [ $RUN_M1_EXPERIMENT -eq 1 ] ;
then
    if [ "$DATASIZE" -eq "0" ] ;
    then
        echo "Please provide a data size to run the experiment."
        exit 1
    fi
M1_EXPERIMENT_DIR="${M1_EXPERIMENT_BASE_DIR}/${DATASIZE}"
INPUT_DIR="${M1_EXPERIMENT_DIR}"
OUTPUT_DIR="${M1_EXPERIMENT_DIR}"
fi

MAX_TEST=65
TEST_IDS=`seq -w 1 ${MAX_TEST}`

if [ "$UPTOMILE" -eq "1" ] ;
then
    MAX_TEST=9
elif [ "$UPTOMILE" -eq "2" ] ;
then
    MAX_TEST=19
elif [ "$UPTOMILE" -eq "3" ] ;
then
    MAX_TEST=44
elif [ "$UPTOMILE" -eq "4" ] ;
then
    MAX_TEST=59
elif [ "$UPTOMILE" -eq "5" ] ;
then
    MAX_TEST=65
fi

function killserver () {
    SERVER_NUM_RUNNING=`ps aux | grep server | wc -l`
    if [ $(($SERVER_NUM_RUNNING)) -ne 0 ]; then
        # kill any servers existing
        if pgrep server; then
            pkill -9 server
        fi
    fi
}

FIRST_SERVER_START=0

for TEST_ID in $TEST_IDS
do
    if [ "$TEST_ID" -le "$MAX_TEST" ]
    then
        if [ ${FIRST_SERVER_START} -eq 0 ]
        then
            cd $BASE_DIR/src
            # start the server before the first case we test.
            ./server > last_server.out &
            FIRST_SERVER_START=1
        elif [ $RUN_M1_EXPERIMENT -eq 1 ] || [ ${TEST_ID} -eq 2 ] || [ ${TEST_ID} -eq 5 ] || [ ${TEST_ID} -eq 11 ] || [ ${TEST_ID} -eq 21 ] || [ ${TEST_ID} -eq 22 ] || [ ${TEST_ID} -eq 31 ] || [ ${TEST_ID} -eq 46 ] || [ ${TEST_ID} -eq 62 ]
        then
            # We restart the server after test 1,4,10,20,21,30,33 (before 2,3,11,12,19,20,31,33), as expected.
            # Also, restart when running M1 Experiment so that all first select queries are run on fresh server.

            killserver

            # start the one server that should be serving test clients
            # invariant: at this point there should be NO servers running
            cd $BASE_DIR/src
            ./server > last_server.out &
            sleep $WAIT_SECONDS_TO_RECOVER_DATA
        fi

        SERVER_NUM_RUNNING=`ps aux | grep server | wc -l`
        if [ $(($SERVER_NUM_RUNNING)) -lt 1 ]; then
            echo "Warning: no server running at this point. Your server may have crashed early."
        fi

        $BASE_DIR/infra_scripts/run_test.sh $TEST_ID $INPUT_DIR $OUTPUT_DIR
        sleep 1
    fi
done

echo "Milestone Run is Complete up to Milestone #: $UPTOMILE"

killserver
