#!/bin/bash

# Script to find and extract all tar.gz files in the current working directory
# Creates a directory named after the file stem for each tar.gz file
# Usage: ./extract.sh

EXTRACTED=0
SKIPPED=0
FAILED=0

while IFS= read -r TARFILE; do
    STEM=$(basename "$TARFILE" .tar.gz)
    
    echo "Processing: $(basename "$TARFILE")"
    
    if [ -d "$STEM" ]; then
        echo "  ✓ Directory '$STEM' already exists, skipping"
        ((SKIPPED++))
    else
        echo "  → Extracting to directory '$STEM'..."
        
        mkdir -p "$STEM"
        
        if tar -xzf "$TARFILE" -C "$STEM"; then
            echo "  ✓ Successfully extracted"
            ((EXTRACTED++))
        else
            echo "  ✗ Extraction failed"
            rmdir "$STEM"  
            ((FAILED++))
        fi
    fi
done < <(find . -maxdepth 1 -name "*.tar.gz" -type f | sort)

# Print summary
echo ""
echo "========================================"
echo "Summary:"
echo "  Extracted: $EXTRACTED"
echo "  Skipped:   $SKIPPED"
echo "  Failed:    $FAILED"
echo "========================================"

if [ $FAILED -gt 0 ]; then
    exit 1
fi
