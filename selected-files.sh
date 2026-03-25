#!/bin/bash

# Configuration
TEMP_PATH="gs://customer-test-20/merged-data-1/temp_parts/layer_1773823380"
FINAL_DEST="gs://customer-test-20/merged-data-1/merged_output.csv"
BATCH_SIZE=32

echo "Starting targeted merge of parts 4 through 9..."

# Function to perform recursive composition
recursive_compose() {
    local folder=$1
    local destination=$2

    # --- MODIFIED SECTION ---
    # Specifically targeting part_4.tmp through part_9.tmp
    # We use a loop to verify existence and build the array
    local files=()
    for i in {4..9}; do
        local file_path="${folder}/part_${i}.tmp"
        # Check if the file exists before adding to the merge list
        if gcloud storage ls "$file_path" >/dev/null 2>&1; then
            files+=("$file_path")
        fi
    done
    # --- END MODIFIED SECTION ---

    local total=${#files[@]}

    if [ "$total" -eq 0 ]; then
        echo "Error: None of the targeted files (part_4 to part_9) found in $folder"
        return 1
    elif [ "$total" -eq 1 ]; then
        echo "Only one targeted file found. Copying to final destination..."
        gcloud storage cp "${files[0]}" "$destination"
        return 0
    fi

    echo "Found $total targeted files. Processing merge..."

    # Since we only have 6 files (4,5,6,7,8,9), and BATCH_SIZE is 32,
    # it will merge them all in one single 'compose' operation.
    local next_layer_folder="$folder/layer_$(date +%s)"

    for ((i=0; i<$total; i+=BATCH_SIZE)); do
        local batch=("${files[@]:i:BATCH_SIZE}")
        local out_file="$next_layer_folder/merged_range.tmp"

        echo "Merging specific batch into $out_file"
        gcloud storage objects compose "${batch[@]}" "$out_file"
    done

    # Final Move
    echo "Moving merged result to $destination"
    gcloud storage cp "$next_layer_folder/merged_range.tmp" "$destination"
}

# Start the process
recursive_compose "$TEMP_PATH" "$FINAL_DEST"

echo "------------------------------------------------"
echo "Targeted merge complete!"
echo "Final file: $FINAL_DEST"