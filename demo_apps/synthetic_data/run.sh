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
	--task-input-size 100 \
	--task-output-size 0 \
	--task-interval 1 \
	--task-count 100 \
	--use-value-server \
	#--config $CONFIG

# Kill the redis server
kill $REDIS

