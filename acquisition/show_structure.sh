#!/bin/bash

. dir_structure.sh

hadoop fs -ls -R -h "$TRIPS_DATA_IN" \
		"$WEATHER_DATA_IN" \
		"$LOGS_DIR" \
		"$RESULT_DIR"