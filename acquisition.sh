#!/bin/bash

DESCR="Script downloads files listed in first column of given list and stores in HDFS in location specified in second column."
USAGE="USAGE: $0 -i=DATA_SRC_DEST_FILE [-e=CSV_DATA_HEADER_FILE] [-l=LOG_FILE] [-g=LOGGING_SCRIPT]"

# default values
LOG_FILE="$PWD/acquisition.log"
CSV_DATA_HEADER_FILE="$PWD/trip_data_header.csv"
LOGGING_SCRIPT="$PWD/bash-logger/src/logger.sh"

# returns absolute path
as_abs() {
	if [[ ! "$1" = /* ]]; then
	    local result="$PWD/$1"
	else
		local result="$1"
	fi
	echo "$result"
}

# parse parameters
for i in "$@"
do
case $i in
    -i=* | --input-list=*)
    	data_src_dest_list=$(as_abs "${i#*=}")
    	;;
    -l=* | --log-file=*)
    	LOG_FILE=$(as_abs "${i#*=}")
    	;;
    -e=* | --csv-header=*)
    	CSV_DATA_HEADER_FILE=$(as_abs "${i#*=}")
    	;;
    -g=* | --log-module=*)
    	LOGGING_SCRIPT=$(as_abs "${i#*=}")
    	;;
    -h | --help)
	    echo "$DESCR"
	    echo "$USAGE"
	    exit 0
	    ;;
esac
done

# import logging functions
source "$LOGGING_SCRIPT" "$LOG_FILE"
echo "Storing logs in file: $LOG_FILE ..."


### Functions

# computes checksum for local file
compute_checksum() {
	if [[ -z "$1" ]]; then
		ERROR "Path not given or empty."
		exit 1
	fi
	local result=$(md5sum < "$1")
	echo "$result"
}


# computes checksum for file stored in HDFS
compute_hdfs_checksum() {
	if [[ -z "$1" ]]; then
		ERROR "Path not given or empty."
		exit 1
	fi
	local result=$(hadoop fs -cat "$1" | md5sum)
	echo "$result"
}


# Performs checking after uploading file to HDFS:
# 	1. checks if given file is present in HDFS
#   2. checks if checksums are same
# $1: HDFS file
# $2: local file's checksum
test_hdfs_file() {
	local filepath="$1"
	local local_checksum="$2"
	if [[ -z "$filepath" ]] || [[ -z "$local_checksum" ]];then
		ERROR "Path to test file not given."
		return 1
	fi
	hadoop fs -test -e "$filepath" \
		|| hadoop fs -test -s "$filepath" \
		|| ( ERROR "File $filepath is not present in HDFS." \
		   && return 1 )

	local remote_checksum=$(compute_hdfs_checksum "$filepath")
	if [[ "$remote_checksum" != "$local_checksum" ]];then
		ERROR "Checksums don't match. Try to upload files again."
		exit 1
	fi
}


test_hdfs_dir() {
	dirpath="$1"
	if [ ! -z "$dirpath" ];then
		hadoop fs -test -d "$dirpath" \
			|| ( ERROR "Directory $dirpath is not present in HDFS."; return 1 )
	else
		ERROR "Path to test not given."
		return 1
	fi
}

# checks if file contains data and has declararation of all required csv fields
verify_data_file() {
	if [[ -z "$1" ]]; then
		ERROR "Path not given or empty."
		exit 1
	fi
	local data_file="$1"
	if [ -z "$data_file" ];then
		ERROR "File doesn't contain any data."
		return 1
	fi

	# check header of file
	if [[ $(head -n 1 "$data_file") != $(cat "$CSV_DATA_HEADER_FILE") ]];then
		ERROR "Invalid CSV file header in file $data_file"
		exit 1
	fi
}

# Downloads file from remote server into local machine, extracts and uploads
# into HDFS. Performs important checks.
remote_to_hdfs() {
	local download_url="$1"
	local upload_dir="$2"
	if [ -z "$download_url" ] || [ -z "$upload_dir" ];then
		ERROR "URL to resource or dest path not given."
		return 1
	fi
	test_hdfs_dir "$upload_dir"
	INFO "Downloading $download_url into $upload_dir ..."
	# store temporary files in /tmp
	cd "/tmp"
	arch_name="temp.zip"
	wget "$download_url" -O "$arch_name"
	# we know that archive contains only one file
	csv_data=$(zipinfo -1 "$arch_name" | head -n 1)
	upload_path="$upload_dir/$csv_data"
	csv_data="/tmp/$csv_data"

	# unzip archive and remove as it is not needed anymore
	unzip -o "$arch_name" && rm -f "$arch_name"
	if [ ! -s "$csv_data" ];then
		echo "File is empty."
		return 1
	fi

	INFO "Change from dos file format (line endings) to unix format ..."
	dos2unix "$csv_data"
	
	# check if file is already in hdfs; if yes then use checksum to determine
	# if should be leaved or overwritten
	local local_checksum=$(compute_checksum "$csv_data")
	if hadoop fs -test -e "$upload_path";then
		remote_checksum=$(compute_hdfs_checksum "$upload_path")
		if [[ "$remote_checksum" = "$local_checksum" ]];then
			WARN "File $upload_path already exists. Nothing will be done."
			INFO "Removing temporary files ..."
			rm -f "$arch_name"
			return 0
		else
			INFO "File $upload_path with different content exists in HDFS. \
			Overwrite old file ..."
			# just remove old file, new file will be uploaded later
			hadoop fs -rm -f -skipTrash "$upload_path"
		fi	
	fi

	INFO "Verify correctness of downloaded file ($download_url) ..."
	verify_data_file "$csv_data"
	
	INFO "Uploading $csv_data to HDFS:${upload_path} ..."
	hadoop fs -moveFromLocal "$csv_data" "$upload_path"
	INFO "Removing temporary files ..."
	rm -f "$csv_data"

	INFO "Check if file $csv_data has been correctly uploaded into HDFS ..."
	test_hdfs_file "$upload_path" "$local_checksum"
	INFO "File $csv_data has been successfully uploaded to $upload_path ."
}


#### main

if [ -z "$data_src_dest_list" ] || [ ! -f "$data_src_dest_list" ];then
	ERROR "Path with URLs to download and upload not given or file not exists."
	INFO "$USAGE"
	INFO "$DESCR"
	exit 1
fi


files_no=$(wc -l "$data_src_dest_list" | awk '{print $1}')
if [ "$files_no" -eq 0 ];then
	WARN "Given list $data_src_dest_list is empty. Nothing will be done."
else
	INFO "Used data header definition: $CSV_DATA_HEADER_FILE"
	INFO "Starting to process $files_no files .."
	while IFS=" " read -r url remote_path; do
		remote_to_hdfs "$url" "$remote_path"
	done < "$data_src_dest_list"
	INFO "Done."
fi
