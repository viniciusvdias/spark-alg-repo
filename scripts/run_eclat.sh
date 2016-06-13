#!/bin/bash

nmachines=$1
nrepl=$2

for i in `seq 1 $nrepl`; do
	echo "eclat: replication $i"
	echo "spark-submit --master yarn-client"
	echo "             --class fim.eclat.Eclat"
	echo "             --driver-memory 6g"
	echo "             --executor-cores 4"
	echo "             --executor-memory 6g"
	echo "             --num-executors $nmachines"
	echo "             fim-spark_2.10-1.0.jar"
	echo "             --inputFile data/t9"
	echo "             --minSupport 0.01"
	echo "             --logLevel info &> eclat.out.$i"

	sleep 5
	spark-submit --master yarn-client \
		     --class fim.eclat.Eclat \
		     --driver-memory 6g \
		     --executor-cores 4 \
		     --executor-memory 6g \
		     --num-executors $nmachines \
		     fim-spark_2.10-1.0.jar \
		     --inputFile data/t9 \
		     --minSupport 0.01 --logLevel info &> eclat.$nmachines.$i
done
