#!/usr/bin/env bash

this=`dirname $0`
this=`cd $this; pwd`
logs=${this}/logs

if [ ! -d ${logs} ]; then
    mkdir -p ${logs}
fi

# appName=

num=$(ps -ef | grep ${appName} | grep -v grep | wc -l)
if [ $num -eq 1 ]; then
    thread=$(ps -ef | grep ${appName} | grep -v grep | awk '{print $2}' | head -n 1)
    echo "Server was already started as $thread."
    exit 0
elif [ $num -gt 1 ]; then
    echo "Warning: More than 1 thread detected."
    exit -1
fi

if ! which java >> /dev/null 2>&1; then
    if [ -n "$JAVA_HOME" ]; then
        java=$JAVA_HOME/bin/java
    else
        echo "JAVA_HOME not set."
	exit 0
    fi
else
    java=`which java`
fi

if [ "$1" == "-daemon" ]; then
    nohup $java -jar $this/app.jar >> $logs/api.log 2>&1 &
    echo "Start success, see logs in $logs/api.log"
else
    $java -jar $this/app.jar
fi

