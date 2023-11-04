#!/bin/bash

#needs work
# Source directory
source_dir="../../cleaned_data/"

# Destination directory with today's date
backup_dir="../../cleaned_data_backup/cleaned_data_$(date +_%Y-%m-%d)/"

# Create the backup directory if it doesn't exist
mkdir -p "$backup_dir"

# Copy the contents of the source directory to the backup directory
cp -r "$source_dir"* "$backup_dir"

# Optional: Print a message indicating the backup was successful
echo "Backup completed from $source_dir to $backup_dir"
