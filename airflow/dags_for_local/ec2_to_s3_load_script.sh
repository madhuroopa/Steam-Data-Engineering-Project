#!/bin/bash

if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install and configure it."
    exit 1
fi


if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source-directory> <s3://bucket-name/destination>"
    exit 1
fi

source_directory="$1"
s3_destination="$2"

# AWS CLI to copy data to S3 with the --recursive option
aws s3 cp "$source_directory" "$s3_destination" --recursive

if [ $? -eq 0 ]; then
    echo "Data copied to S3 successfully."
else
    echo "Error: Data copy to S3 failed."
    exit 1
fi
