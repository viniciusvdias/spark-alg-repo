#! /usr/bin/env sh

nrepl=5

for i in `seq 1 1 8`; do
	./run_eclat_scaleup.sh t$i $nrepl
	./run_twidd_scaleup.sh t$i $nrepl
done

#for nmachines in `seq 2 2 2`; do
#	./run_eclat.sh $nmachines $nrepl 
#	./run_twidd.sh $nmachines $nrepl 
#done
