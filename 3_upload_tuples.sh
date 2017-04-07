#!/bin/bash

java -cp "dependencies/*" -Xmx40G \
bulkload.UploadTuples "$@"

exit 0
