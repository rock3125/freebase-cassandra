#!/bin/bash

java -cp "dependencies/*" -Xmx40G \
bulkload.UploadVocab "$@"

exit 0
