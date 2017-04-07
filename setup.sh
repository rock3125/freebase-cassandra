#!/bin/bash

gradle clean build copyRuntimeLibs
mkdir -p dependencies
cp build/libs/*.jar dependencies/
cp build/dependencies/* dependencies/
