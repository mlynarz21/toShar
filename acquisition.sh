#!/bin/bash

DESCR="Script downloads files listed in first column of given file and stores 
in HDFS in location specified in second column."
USAGE="USAGE: $0 -i=DATA_SRC_DEST_FILE [-e=CSV_DATA_HEADER_FILE] [-l=LOG_FILE] 
[-a=CSV_DATA_ALT_HEADER_FILE] [-t=TIMEOUT_IN_SECS] [-r=TRIES_NUMBER] 
[--remove-cr] [--replace-cr] [-g=LOGGING_SCRIPT]"

# default values
LOG_FILE="$PWD/acquisition.log"
# CSV_DATA_HEADER_FILE="$PWD/trip_data_header.csv"
# CSV_DATA_ALT_HEADER_FILE="$PWD/trip_data_header_2.csv"
LOGGING_SCRIPT="$PWD/bash-logger/src/logger.sh"
timeout=30  # seconds
retry=3  # times
remove_cr=''
replace_cr=''

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
    -a=* | --csv-alt-header=*)
       	CSV_DATA_ALT_HEADER_FILE=$(as_abs "${i#*=}")
       	;;
    -t=* | --timeout=*)
       	timeout="${i#*=}"
       	;;
    -r=* | --retry=*)
       	retry="${i#*=}"
       	;;
    -g=* | --log-module=*)
    	LOGGING_SCRIPT=$(as_abs "${i#*=}")
    	;;
    --remove-cr)
   	    remove_cr='yes'
   	    ;;
   	--replace-cr)
   	    replace_cr='yes'
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


rm_hdfs_file() {
	hadoop fs -rm -f -skipTrash "$1"
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
		# clean before exit
		rm_hdfs_file "$filepath"
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

check_hdfs_connection() {
	set -o pipefail
	INFO "Checking connection to HDFS ..."
	hadoop fs -ls /user/cloudera/in 3>&1 2>&3 >/dev/null | tee -a "$LOG_FILE"
	if [ $? -ne 0 ];then
		ERROR "Cannot connect to HDFS."
		exit 1
	fi
	set +o pipefail
}

# Changes format of files: removes quoting and changes to lowercase to keep
# same format for all files (there are some inconsistencies in source files).
# Changes file in place (save under same name).
# Fails if file is empty.
prepare_data_file() {
	if [[ -z "$1" ]]; then
		ERROR "Path not given or empty."
		exit 1
	fi
	local data_file="$1"
	if [ -z "$data_file" ];then
		ERROR "File doesn't contain any data."
		# clean before exit
		rm -f "$data_file"
		return 1
	fi

	# lowercase fields in header and remove quoting
	awk '{ if (NR == 1) {print tolower($0)} else print }' "$data_file" \
		| tr -d '"' > /tmp/data.tmp \
		&& mv /tmp/data.tmp "$data_file"
}

# checks if file contains data and has declararation of all required csv fields.
# While verifying header, checks against two versions of header (if alternative
# version header given). In case of matching header preserves unquoted, 
# lowercased header.
verify_data_file() {
	if [[ -z "$1" ]]; then
		ERROR "Path not given or empty."
		exit 1
	fi
	local data_file="$1"
	
	src_file_header=$(head -n 1 "$data_file")
	if [[ "$src_file_header" != $(cat "$CSV_DATA_HEADER_FILE") ]];then
		WARN "CSV file header in file $data_file not match $CSV_DATA_HEADER_FILE"
		if [[ -n "$CSV_DATA_ALT_HEADER_FILE" ]];then
			INFO "Checking alternative version of header ..."			
			if [[ "$src_file_header" = $(cat "$CSV_DATA_ALT_HEADER_FILE") ]];then
				INFO "Change header from alternative version to preferred ..."
				cat "$CSV_DATA_HEADER_FILE" <(tail -n +2 "$data_file") > /tmp/with_changed_header.tmp \
					&& mv /tmp/with_changed_header.tmp "$data_file"
				return 0
			fi
		fi
		ERROR "Invalid CSV file header in file $data_file:
		Expected: $(cat $CSV_DATA_HEADER_FILE)
		Or: $(cat $CSV_DATA_ALT_HEADER_FILE)
		Was: $src_file_header"

		# clean before exit
		rm -f "$data_file"
		exit 1
	fi
}

adjust_textfile_format() {
	if [[ -n "$replace_cr" ]];then
		INFO "Change line endings to unix format (replace carriage-return with newline) ..."
		cat "$csv_data" | tr "\r" "\n" > /tmp/data_fmt.tmp \
			&& mv /tmp/data_fmt.tmp "$csv_data"
	fi
	if [[ -n "$remove_cr" ]];then
		INFO "Change line endings to unix format (remove carriage-return) ..."
		cat "$csv_data" | tr -d "\r" > /tmp/data_fmt.tmp \
			&& mv /tmp/data_fmt.tmp "$csv_data"
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

	check_hdfs_connection

	test_hdfs_dir "$upload_dir"
	INFO "Downloading $download_url into $upload_dir ..."
	# store temporary files in /tmp
	cd "/tmp"
	arch_name="temp.zip"
	wget "$download_url" --timeout $timeout --tries $retry -O "$arch_name"
	if [ $? -ne 0 ];then
		ERROR "Cannnot download file $download_url . Check internet connections \
		and try again."
		rm -f "$download_url"
		exit 1
	fi
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

	# adjust format if needed
	adjust_textfile_format "$csv_data"
	
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


	INFO "Prepare format of downloaded file ($download_url) ..."
	prepare_data_file "$csv_data"
	INFO "Verify correctness of downloaded file ($download_url) ..."
	verify_data_file "$csv_data"

	local local_chg_checksum=$(compute_checksum "$csv_data")
	INFO "Uploading $csv_data to HDFS:${upload_path} ..."
	hadoop fs -moveFromLocal "$csv_data" "$upload_path"
	INFO "Removing temporary files ..."
	rm -f "$csv_data"

	INFO "Check if file $csv_data has been correctly uploaded into HDFS ..."
	test_hdfs_file "$upload_path" "$local_chg_checksum"
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
