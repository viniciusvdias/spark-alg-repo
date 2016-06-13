#!/bin/bash

logfile=$1

./run_opt.sh $logfile $SPARK_HOME/bin/spark-submit --master local[*] \
   --class dmining.fim.fptree.Twidd \
   ../target/scala-2.10/spark-alg-repo_2.10-1.0.jar \
   --inputFile ../input/twig-input.txt --minSupport 0.0001
