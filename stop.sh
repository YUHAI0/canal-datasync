#!/usr/bin/env bash

# appName=
ps -ef | grep ${appName}| grep -v grep | awk '{print $2}' | xargs kill -9
