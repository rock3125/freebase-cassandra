#!/bin/bash

java -cp "dependencies/*" \
bulkload.ConvertFreebase "$@"

exit 0
