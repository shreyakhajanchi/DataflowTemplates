#!/bin/bash

# Configuration
TEMP_PATH="gs://customer-test-20/merged-data-1/temp_parts"
FINAL_DEST="gs://customer-test-20/merged-data-1/merged_output.csv"
BATCH_SIZE=32

echo "Starting recursive merge of temporary parts..."

# Function to perform recursive composition
recursive_compose() {
    local folder=$1
    local destination=$2

    # Get current list of files in this specific temp folder
    local files=($(gcloud storage ls "$folder/*.tmp" | grep -v "/$"))
    local total=${#files[@]}

    if [ "$total" -eq 0 ]; then
        echo "Error: No .tmp files found in $folder"
        return 1
    elif [ "$total" -eq 1 ]; then
        echo "Only one file remains. Moving to final destination..."
        gcloud storage cp "${files[0]}" "$destination"
        return 0
    fi

    echo "Found $total files. Processing next layer of merge..."

    local next_layer_folder="$folder/layer_$(date +%s)"
    local next_files=()

    for ((i=0; i<$total; i+=BATCH_SIZE)); do
        local batch=("${files[@]:i:BATCH_SIZE}")
        local out_file="$next_layer_folder/part_$((i/BATCH_SIZE)).tmp"

        echo "Merging batch starting at index $i into $out_file"
        gcloud storage objects compose "${batch[@]}" "$out_file"
        next_files+=("$out_file")
    done

    # Recursive call: process the new layer we just created
    recursive_compose "$next_layer_folder" "$destination"
}

# Start the process
recursive_compose "$TEMP_PATH" "$FINAL_DEST"

echo "------------------------------------------------"
echo "Recursive merge complete!"
echo "Final file: $FINAL_DEST"