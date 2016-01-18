#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./run.sh  <port>"
    exit 1
fi

${JAVA_HOME}/bin/java Server $1 

