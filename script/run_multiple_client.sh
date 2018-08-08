#!/bin/bash

if [ "$#" -ne 3 ]; then
	echo "Please input one query file and worker range"
	echo "./run_multiple_client.sh <start_worker> <end_worker> <query_file>"
	exit 1 
fi

for i in $(seq $1 $2)
do
	ssh worker$i 'cd /data/aaron/gquery && ./release/client /data/aaron/gquery/5ib_conf '$3' > output/output'$i' && exit'
done;

python3 /data/aaron/gquery/script/parse_calculate.py $1 $2
