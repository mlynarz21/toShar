#!/bin/bash

DESCR="Script downloads files listed in first column of given list and stores in HDFS in location specified in second column."
USAGE="USAGE: $0 FILES_DEST_DIR"

ROOT_DIR='/user/cloudera'
TRIPS_DATA_IN="$ROOT_DIR/in/trips"
WEATHER_DATA_IN="$ROOT_DIR/in/weather"
LOGS_DIR="$ROOT_DIR/logs"
RESULT_DIR="$ROOT_DIR/out"

FILES_DEST_DIR="$1"
if [ -z "$FILES_DEST_DIR" ] || [ ! -f "$FILES_DEST_DIR" ];then
	echo "Path with URLs to download and upload not given or file not exists."
	echo "$USAGE"
	echo "$DESCR"
	exit 1
fi


test_hdfs_file() {
	filepath="$1"
	if [ ! -z "$filepath" ];then
		hadoop fs -test -e "$filepath" \
			|| hadoop fs -test -s "$filepath" \
			|| ( echo "File $filepath is not present in HDFS." \
			   && return 1 )
	else
		echo "Path to test not given."
		return 1
	fi
}

test_hdfs_dir() {
	dirpath="$1"
	if [ ! -z "$dirpath" ];then
		hadoop fs -test -d "$dirpath" \
			|| ( echo "Directory $dirpath is not present in HDFS." \
			   && return 1 )
	else
		echo "Path to test not given."
		return 1
	fi
}

download_file() {
	download_url="$1"
	upload_dir="$2"
	if [ -z "$download_url" ] || [ -z "$upload_dir" ];then
		echo "URL to resource or dest path not given."
		return 1
	fi
	test_hdfs_dir "$upload_dir"
	echo "Downloading $download_url into $upload_dir ..."
	# store temporary files in /tmp
	cd "/tmp"
	arch_name="temp.zip"
	wget "$download_url" -O "$arch_name"
	csv_data=$(zipinfo -1 "$arch_name" | head -n 1)
	upload_path="$upload_dir/$csv_data"

	# unzip archive and remove as it is not needed anymore
	unzip -o "$arch_name" && rm -f "$arch_name"
	if [ ! -s "$csv_data" ];then
		echo "File is empty."
		return 1
	fi
	
	# check if file is already in hdfs; if yes then use checksum to determine
	# if should be leaved or overwritten
	hadoop fs -test -e "$upload_path"
	if [[ "$?" -eq 0 ]];then
		remote_checksum=$(hadoop fs -cat "$upload_path" | md5sum)
		local_checksum=$(md5sum < "$csv_data")
		if [[ "$remote_checksum" = "$local_checksum" ]];then
			echo "File $upload_path already exists. Nothing will be done."
			echo "Removing temporary files ..."
			rm -f "$arch_name"
			return 0
		else
			echo "File $upload_path with different content exists in HDFS. Overwrite old file ..."
			hadoop fs -rm -f -skipTrash "$upload_path"
		fi	
	fi

	if [ -z "$csv_data" ];then
		echo "Archive doesn't contain any data."
		return 1
	fi
	
	echo "Uploading $csv_data to $upload_path ..."
	hadoop fs -moveFromLocal "$csv_data" "$upload_path"
	echo "Removing temporary files ..."
	rm -f "$csv_data"
	# echo "$arch_name" "$csv_data"
	
	# hadoop fs -test -e "$upload_path" \
		# || hadoop fs -test -s "$upload_path" \
		# || ( echo "File $csv_data was not uploaded to $upload_path ." \
		   # && return 1 )
	test_hdfs_file "$upload_path"
	echo "File $csv_data was uploaded to $upload_path ."
}


# main

files_no=$(wc -l "$FILES_DEST_DIR" | awk '{print $1}')
if [ "$files_no" -eq 0 ];then
	echo "Given list is empty."
else
	echo "Processing $files_no files .."
	while IFS=" " read -r url remote_path; do
		download_file "$url" "$remote_path"
	done < "$FILES_DEST_DIR"
	echo "Done."
fi
