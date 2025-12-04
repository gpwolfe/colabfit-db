#!/bin/bash

mkdir -p already_tarred

for dir in DS_*/; do
    if [ -d "$dir" ]; then
        tarfile="tarfiles/${dir%/}.tar.gz"
        if [ -f "$tarfile" ]; then
            echo "Moving $dir to already_tarred/ (tar file exists: $tarfile)"
            mv "$dir" already_tarred/
        else
            echo "No tar file found for $dir - keeping in place"
        fi
    fi
done

echo "Done moving tarred directories!"
