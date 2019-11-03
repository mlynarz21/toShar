#!/bin/bash

. dir_structure.sh

make_dir_structure () {
	hadoop fs -mkdir -p  "$TRIPS_DATA_IN" \
		"$WEATHER_DATA_IN" \
		"$LOGS_DIR" \
		"$RESULT_DIR"
}

echo "Initializing project structure ..."
make_dir_structure
