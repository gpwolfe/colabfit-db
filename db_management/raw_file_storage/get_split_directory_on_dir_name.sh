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


find "$SOURCE_DIR" -type d -name "*${SUBSTRING}*" | while read -r dir; do
    
    relative_path="${dir
    
    
    target_dir="$TARGET_DIR/$relative_path"
    mkdir -p "$target_dir"
    
    
    cp -r "$dir/"* "$target_dir" 2>/dev/null
done

echo "Directories containing '${SUBSTRING}' in their names have been copied to '$TARGET_DIR'."
