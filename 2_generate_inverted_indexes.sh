#!/bin/bash

java -cp "dependencies/*" -Xmx40G \
bulkload.CreateInvertedIndices "$@"

exit 0
