#!/bin/bash

logfile=$1
shift

optPlan=$(python analyzer/defaultanalyzer.py $logfile)

echo "exec $@ --optPlan $optPlan"
exec $@ --optPlan $optPlan
