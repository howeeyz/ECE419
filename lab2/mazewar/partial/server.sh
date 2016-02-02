#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: ./server.sh  <port> <num players>"
    exit 1
fi

${JAVA_HOME}/bin/java Server $1 $2 

