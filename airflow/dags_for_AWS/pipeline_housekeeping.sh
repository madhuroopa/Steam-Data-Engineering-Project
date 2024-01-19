#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <directory_path>"
    exit 1
fi

directory_to_delete="$1"

# Check if the directory exists
if [ -d "$directory_to_delete" ]; then
    rm -r "$directory_to_delete"
    echo "Deleted $directory_to_delete"
else
    echo "Directory $directory_to_delete does not exist."
fi
