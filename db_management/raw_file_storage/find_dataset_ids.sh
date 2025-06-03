#!/bin/bash

today=$(date +"%Y_%m_%d")

output_file="dataset_ids_${today}.csv"

find /xyz/original -type f -name "DS*.tar*" -size +0c -exec basename {} .tar.xz \; > "$output_file"
echo "Search complete. Results saved to $output_file"
