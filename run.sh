#!/bin/bash

WORKDIR="$PWD"
LOG_FILE="$WORKDIR/acquisition.log"
# LOG_FILE="hdfs:/user/cloudera/logs/acquisition.log"
WEATHER_SRC_DATA="$WORKDIR/weather_source.txt"
WEATHER_CSV_DATA_HEADER_FILE="$WORKDIR/weather_data_header.csv"
BIKE_TRIP_SRC_DATA="$WORKDIR/bike_trip_sources.txt"
BIKE_TRIP_CSV_DATA_HEADER_FILE="$WORKDIR/trip_data_header.csv"
BIKE_TRIP_CSV_DATA_ALT_HEADER_FILE="$WORKDIR/trip_data_header_2.csv"

LOGGING_SCRIPT="$WORKDIR/bash-logger/src/logger.sh"
# import logging functions

echo "Recreating project structure..."
./clean.sh && ./initialize.sh

source "$LOGGING_SCRIPT" "$LOG_FILE"
echo "Storing logs in file: $LOG_FILE ..."

INFO "Starting acquisition for weather data..."
./acquisition.sh -i="$WEATHER_SRC_DATA" \
	--csv-header="$WEATHER_CSV_DATA_HEADER_FILE" \
	--log-file="$LOG_FILE" \
	--replace-cr

cd "$WORKDIR"
INFO "Starting acquisition for bike trips data..."
./acquisition.sh -i="$BIKE_TRIP_SRC_DATA" \
	--csv-header="$BIKE_TRIP_CSV_DATA_HEADER_FILE" \
	--csv-alt-header="$BIKE_TRIP_CSV_DATA_ALT_HEADER_FILE"\
	--log-file="$LOG_FILE" \
	--remove-cr

INFO "Acquisition process completed."
