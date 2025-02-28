#!/bin/bash


if [[ $
    echo "Usage: $0 <source_directory> <substring>"
    exit 1
fi


SOURCE_DIR="$1"
SUBSTRING="$2"


if [[ ! -d "$SOURCE_DIR" ]]; then
    echo "Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi


TARGET_DIR="${SOURCE_DIR}_${SUBSTRING}"


mkdir -p "$TARGET_DIR"


find "$SOURCE_DIR" -type f -name "*${SUBSTRING}*" | while read -r file; do
    
    relative_path="${file
    
    
    target_dir="$TARGET_DIR/$(dirname "$relative_path")"
    
    
    mkdir -p "$target_dir"
    
    
    cp "$file" "$target_dir"
done

echo "Files containing '${SUBSTRING}' in their names have been copied to '$TARGET_DIR'."
