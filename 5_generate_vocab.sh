#!/bin/bash

java -cp "dependencies/*" \
bulkload.UploadVocab "$@"

exit 0
