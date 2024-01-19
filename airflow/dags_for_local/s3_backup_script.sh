#!/bin/bash

#S3 main to S3 backup

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source_bucket> <destination_bucket>"
    exit 1
fi

source_bucket="$1"
destination_bucket="$2"

aws s3 sync "s3://${source_bucket}" "s3://${destination_bucket}" --exclude "*.tmp"