#!/bin/bash

WORKDIR="$(cd .. && pwd)"
echo "$WORKDIR"
TESTDIR="$PWD"
LOG_FILE="$TESTDIR/acquisition_test.log"
BIKE_TRIP_SRC_DATA="$TESTDIR/source_test.txt"
BIKE_TRIP_CSV_DATA_HEADER_FILE="$WORKDIR/trip_data_header.csv"
BIKE_TRIP_CSV_DATA_ALT_HEADER_FILE="$WORKDIR/trip_data_header_2.csv"

rm -f "$LOG_FILE"
LOGGING_SCRIPT="$WORKDIR/bash-logger/src/logger.sh"
# import logging functions
source "$LOGGING_SCRIPT" "$LOG_FILE"
echo "Storing logs in file: $LOG_FILE ..."

cd "$WORKDIR"
INFO "Recreating project structure..."
./clean.sh && ./initialize.sh

INFO "Starting acquisition for test bike trips data..."
./acquisition.sh -i="$BIKE_TRIP_SRC_DATA" \
	--csv-header="$BIKE_TRIP_CSV_DATA_HEADER_FILE" \
	--csv-alt-header="$BIKE_TRIP_CSV_DATA_ALT_HEADER_FILE"\
	--log-file="$LOG_FILE" \
	--remove-cr

RESULT_FILE='/user/cloudera/in/trips/201601-citibike-tripdata.csv'
echo "Acquisition process completed. Logs file: $LOG_FILE"
echo "Looking for file $RESULT_FILE in HDFS ..."
if hadoop fs -test -e "$RESULT_FILE";then
	echo "File is present:"
	hadoop fs -ls "$RESULT_FILE"
else
	echo "File $RESULT_FILE is not present in HDFS."
fi

cd - > /dev/null