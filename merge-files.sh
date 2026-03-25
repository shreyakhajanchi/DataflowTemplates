#!/bin/bash

# Configuration
SOURCE_PATH="gs://customer-test-20/exported-data-1"
DEST_PATH="gs://customer-test-20/merged-data-1/merged_output.csv"
TEMP_PATH="gs://customer-test-20/merged-data-1/temp_parts"
BATCH_SIZE=32

echo "Starting merge process..."

# 1. Get a list of all files in the source directory
FILES=($(gcloud storage ls "$SOURCE_PATH/*" | grep -v "/$"))
TOTAL_FILES=${#FILES[@]}

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo "No files found in $SOURCE_PATH"
    exit 1
fi

echo "Found $TOTAL_FILES files. Merging in batches of $BATCH_SIZE..."

# 2. Compose files 32 at a time into intermediate files
INTERMEDIATE_FILES=()
for ((i=0; i<$TOTAL_FILES; i+=BATCH_SIZE)); do
    BATCH=("${FILES[@]:i:BATCH_SIZE}")
    PART_NAME="$TEMP_PATH/part_$((i/BATCH_SIZE)).tmp"

    echo "Creating intermediate part: $PART_NAME"
    gcloud storage objects compose "${BATCH[@]}" "$PART_NAME"
    INTERMEDIATE_FILES+=("$PART_NAME")
done

# 3. Final merge of the intermediate parts
echo "Performing final merge into $DEST_PATH..."
gcloud storage objects compose "${INTERMEDIATE_FILES[@]}" "$DEST_PATH"

# 4. Cleanup temporary files


echo "Success! Merged file is available at: $DEST_PATH"