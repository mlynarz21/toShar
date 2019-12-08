#!/bin/bash

. dir_structure.sh

remove_dir_structure () {
	hadoop fs -rm -f -r -skipTrash "$TRIPS_DATA_IN" \
		"$WEATHER_DATA_IN" \
		"$LOGS_DIR" \
		"$RESULT_DIR"
}

echo "Removing project structure ..."
remove_dir_structure
