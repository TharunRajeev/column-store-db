#!/bin/bash

# JUST RUN THE SPECIFIED MILESTONE's EXPERIMENT (and potentially test cases)

# Set base directory based on the location of this script, to allow this script to work in both local and container
# environments. As long as this script is in `infra_scripts` directory, this will point to the project root directory.
BASE_DIR=$(dirname $(dirname $(realpath $0)))
INPUT_DIR=
OUTPUT_DIR=

# Input Parameters
UPTOMILE="${1:-5}"
PARAM_VAL="${2:-0}"
WAIT_SECONDS_TO_RECOVER_DATA="${3:-3}"

# Additional flag to track data movements
TRACK_DATA_MOVS=0
# Check if the user passed --track-data-movs anywhere in the arguments
for arg in "$@"; do
    if [ "$arg" = "--track-data-movs" ]; then
        TRACK_DATA_MOVS=1
    fi
done

if [ "$PARAM_VAL" -eq "0" ] ;
then
    echo "Please provide a data size to run the experiment."
    exit 1
fi

EXPERIMENT_DATA_DIR="${BASE_DIR}/experiments/data"

# Directories for each milestone
M1_EXPERIMENT_DIR="${EXPERIMENT_DATA_DIR}/milestone1/${PARAM_VAL}"
M2_EXPERIMENT_DIR="${EXPERIMENT_DATA_DIR}/milestone2/batch_size_${PARAM_VAL}"
M3_EXPERIMENT_DIR="${EXPERIMENT_DATA_DIR}/milestone3"

START_TEST=
MAX_TEST=65
TEST_IDS=$(seq -w 1 ${MAX_TEST})

if [ "$UPTOMILE" -eq "1" ] ;
then
    START_TEST=1
    MAX_TEST=2
    INPUT_DIR="${M1_EXPERIMENT_DIR}"
    OUTPUT_DIR="${M1_EXPERIMENT_DIR}"
elif [ "$UPTOMILE" -eq "2" ] ;
then
    START_TEST=16
    MAX_TEST=19
    INPUT_DIR="${M2_EXPERIMENT_DIR}"
    OUTPUT_DIR="${M2_EXPERIMENT_DIR}"
elif [ "$UPTOMILE" -eq "3" ] ;
then
    START_TEST=33
    MAX_TEST=44
    INPUT_DIR="${M3_EXPERIMENT_DIR}"
    OUTPUT_DIR="${M3_EXPERIMENT_DIR}"
elif [ "$UPTOMILE" -eq "4" ] ;
then
    MAX_TEST=59
elif [ "$UPTOMILE" -eq "5" ] ;
then
    MAX_TEST=65
fi

function killserver () {
    SERVER_NUM_RUNNING=$(ps aux | grep server | wc -l)
    if [ $((SERVER_NUM_RUNNING)) -ne 0 ]; then
        # kill any servers existing
        if pgrep server > /dev/null; then
            pkill -9 server
        fi
    fi
}

SERVER_RUN=0

for TEST_ID in $TEST_IDS
do
    if [ "$TEST_ID" -ge "$START_TEST" ] && [ "$TEST_ID" -le "$MAX_TEST" ] ;
    then
        if [ ${SERVER_RUN} -eq 0 ]
        then
            cd $BASE_DIR/src
            # Start the server before the first case we test.
            SERVER_RUN=1

            if [ $TRACK_DATA_MOVS -eq 1 ]; then
                # If tracking data moves and we are at M1 (where server restarts per test),
                # we'll produce a unique cachegrind file for each test.
                if [ "$UPTOMILE" -eq "1" ]; then
                    VALGRIND_OUT_FILE="${OUTPUT_DIR}/server_valgrind_${TEST_ID}.out"
                    valgrind --tool=cachegrind --cachegrind-out-file="${VALGRIND_OUT_FILE}" ./server > last_server.out &
                else
                    # Single run for other milestones
                    VALGRIND_OUT_FILE="${OUTPUT_DIR}/server_valgrind.out"
                    valgrind --tool=cachegrind --cachegrind-out-file="${VALGRIND_OUT_FILE}" ./server > last_server.out &
                fi
            else
                # Normal run without tracking data movements
                ./server > last_server.out &
            fi
        elif [ "$UPTOMILE" -eq "1" ]
        then
            # Restart when running M1 Experiment so that all first select queries are on a fresh server.
            killserver

            cd $BASE_DIR/src
            # Start again with or without valgrind depending on TRACK_DATA_MOVS
            if [ $TRACK_DATA_MOVS -eq 1 ]; then
                VALGRIND_OUT_FILE="${OUTPUT_DIR}/server_valgrind_${TEST_ID}.out"
                valgrind --tool=cachegrind --cachegrind-out-file="${VALGRIND_OUT_FILE}" ./server > last_server.out &
            else
                ./server > last_server.out &
            fi
            sleep $WAIT_SECONDS_TO_RECOVER_DATA
        fi

        SERVER_NUM_RUNNING=$(ps aux | grep server | wc -l)
        if [ $((SERVER_NUM_RUNNING)) -lt 1 ]; then
            echo "Warning: no server running at this point. Your server may have crashed early."
        fi

        $BASE_DIR/infra_scripts/run_test.sh $TEST_ID $INPUT_DIR $OUTPUT_DIR
        sleep 1
    fi
done

echo "Milestone Run is Complete up to Milestone #: $UPTOMILE"

killserver
