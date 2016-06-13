#!/bin/bash

nmachines=$1
nrepl=$2

for i in `seq 1 $nrepl`; do
	echo "twidd: replication $i"
	echo "spark-submit --master yarn-client"
	echo "             --class fim.fptree.Twidd"
	echo "             --driver-memory 6g"
	echo "             --executor-cores 4"
	echo "             --executor-memory 6g"
	echo "             --num-executors $nmachines"
	echo "             fim-spark_2.10-1.0.jar"
	echo "             --inputFile data/t9"
	echo "             --numPartitions 128,128,128 "
	echo "             --mu 3 --rho 2"
	echo "             --minSupport 0.01"
	echo "             --logLevel info &> twidd.out.$i"

	sleep 5
	spark-submit --master yarn-client \
		     --class fim.fptree.Twidd \
		     --driver-memory 6g \
		     --executor-cores 4 \
		     --executor-memory 6g \
		     --num-executors $nmachines \
		     fim-spark_2.10-1.0.jar \
		     --inputFile data/t9 \
		     --numPartitions 128,128,128 \
		     --mu 3 --rho 2 \
		     --minSupport 0.01 \
		     --logLevel info &> twidd.$i.$nmachines
done
