#!/bin/bash

# Script to copy tar.gz files from a remote pod if they don't exist locally
# Usage: ./copy_tarfiles.sh <file_list.txt>
# File list should contain one tarfile name per line (e.g., DS_0000.tar.gz)

# Configuration
POD_NAME="<pod-name>"
REMOTE_PATH="/xyz/parquet"

# Check if file list argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <file_list.txt>"
    echo "Example: $0 tarfiles.txt"
    exit 1
fi

FILE_LIST="$1"

# Check if the file list exists
if [ ! -f "$FILE_LIST" ]; then
    echo "Error: File list '$FILE_LIST' not found"
    exit 1
fi

# Initialize counters
COPIED=0
SKIPPED=0
FAILED=0

# Read the file list and process each tarfile
while IFS= read -r TARFILE || [ -n "$TARFILE" ]; do
    # Skip empty lines and lines starting with #
    [[ -z "$TARFILE" || "$TARFILE" =~ ^# ]] && continue
    
    # Trim whitespace
    TARFILE=$(echo "$TARFILE" | xargs)
    
    echo "Processing: $TARFILE"
    
    # Check if file already exists locally
    if [ -f "$TARFILE" ]; then
        echo "  ✓ Already exists locally, skipping"
        ((SKIPPED++))
    else
        echo "  → Copying from pod..."
        
        # Copy from remote pod
        if kubectl cp "$POD_NAME:$REMOTE_PATH/$TARFILE" "./$TARFILE"; then
            echo "  ✓ Successfully copied"
            ((COPIED++))
        else
            echo "  ✗ Failed to copy"
            ((FAILED++))
        fi
    fi
done < "$FILE_LIST"

# Print summary
echo ""
echo "========================================"
echo "Summary:"
echo "  Copied:  $COPIED"
echo "  Skipped: $SKIPPED"
echo "  Failed:  $FAILED"
echo "========================================"

# Exit with error if any copies failed
if [ $FAILED -gt 0 ]; then
    exit 1
fi
