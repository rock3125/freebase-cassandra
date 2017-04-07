#!/bin/bash

java -cp "dependencies/*" \
bulkload.UploadIndexes "$@"

exit 0
