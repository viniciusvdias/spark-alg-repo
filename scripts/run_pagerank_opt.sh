#!/bin/bash

logfile=$1

./run_opt.sh $logfile $SPARK_HOME/bin/spark-submit --master local[*] \
   --class ganalytics.PageRank \
   ../target/scala-2.10/spark-alg-repo_2.10-1.0.jar \
   ../input/citeseer-no-label.txt 10,10,10 10 sparkPageRank
