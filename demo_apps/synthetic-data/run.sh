#!/bin/bash
#COBALT -A CSC249ADCD08
#COBALT -t 60
#COBALT -n 1
#COBALT -q debug-flat-quad
#COBALT --attrs enable_shh=0

CONFIG='config.json'

module load miniconda-3/latest
conda activate colmena

# Start the redis server
PORT=59465
redis-server --port $PORT --protected-mode no &> redis.out &
REDIS=$!

echo "Redis started on $HOSTNAME:$PORT"

python synthetic.py \
	--redis-host $HOSTNAME \
	--redis-port $PORT \
	--task-input-size 50 \
	--task-output-size 0 \
	--task-interval 15 \
	--task-count 50 \
    --output-dir runs/test \
    --reuse-data \
	--use-value-server \
	#--config $CONFIG
    #--output-dir runs/full_test_unique_30s_50x50 \

# Kill the redis server
kill $REDIS

