#!/bin/bash

mkdir -p tarfiles

for dir in DS_*/; do
    if [ -d "$dir" ]; then
        if find "$dir" -maxdepth 1 -name "dataset.json" -print -quit | grep -q .; then
            tarfile="tarfiles/${dir%/}.tar.gz"
            if [ -f "$tarfile" ]; then
                echo "Archive $tarfile already exists - skipping $dir"
            else
                echo "Creating archive for $dir"
                tar --warning=no-file-changed -czf "$tarfile" "$dir"
            fi
	else
            echo "No dataset.json file found in $dir - skipping"
        fi
    fi
done

echo "Done!"
