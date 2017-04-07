#!/bin/bash

java -cp "dependencies/*" -Xmx40G \
bulkload.ConvertFreebase "$@"

exit 0
