#!/bin/bash

# tiny script to facilitate generating list of sources to feed into acqusation.sh

TRIP_DATA_PATH='/user/cloudera/in/trips'
echo -e https://s3.amazonaws.com/tripdata/{2016..2016}{01..12}-citibike-tripdata.zip" $TRIP_DATA_PATH\n" \
	| sed 's/^\s//' \
	| grep -Pv '^$'