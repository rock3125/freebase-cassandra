#!/bin/bash

java -cp "dependencies/*" \
bulkload.UploadTuples "$@"

exit 0
