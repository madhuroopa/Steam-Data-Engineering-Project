#!/bin/bash
#  command -v aws checks whether the AWS CLI is installed by attempting to locate the executable in the system's PATH.
if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install and configure it."
    exit 1
fi

# checks whether it received the correct number of command-line arguments. If not (i.e., not equal to 2), 
#it prints a usage message and exits with a status code of 1.
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source-directory> <s3://bucket-name/destination>"
    exit 1
fi

source_directory="$1"
s3_destination="$2"

# AWS CLI to copy data to S3 with the --recursive option, ensures that the entire directory structure is copied.
aws s3 cp "$source_directory" "$s3_destination" --recursive
#the script checks the exit status ($?). If the exit status is 0 (indicating success), it prints a success message.
#Otherwise, it prints an error message and exits with a status code of 1.
if [ $? -eq 0 ]; then
    echo "Data copied to S3 successfully."
else
    echo "Error: Data copy to S3 failed."
    exit 1
fi
