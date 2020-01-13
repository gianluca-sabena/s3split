#!/bin/bash

declare DEST_PATH=""
read -r -p "Destination folder: " DEST_PATH

declare NUM_FILES=""
read -r -p "Enter number of files: " NUM_FILES

declare FILE_SIZE=""
read -r -p "Enter file size in KB: " FILE_SIZE


counter=1; 
SIZE=$((1024 * FILE_SIZE ))
mkdir -p "${DEST_PATH}"
echo "${SIZE}"
while [[ $counter -le $NUM_FILES ]]; 
 do echo Creating file no $counter;
  head -c "$SIZE"  /dev/urandom > "${DEST_PATH}/file_${counter}.txt"
  ((counter += 1))
 done