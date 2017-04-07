#!/bin/bash

java -cp "dependencies/*" -Xmx40G \
bulkload.UploadIndexes "$@"

exit 0
