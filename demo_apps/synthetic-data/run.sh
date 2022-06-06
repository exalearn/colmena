#!/bin/bash
#COBALT -A CSC249ADCD08
#COBALT -t 60
#COBALT -n 1
#COBALT -q debug-flat-quad
#COBALT --attrs enable_shh=0

module load miniconda-3/latest
conda activate colmena

# Start the redis server
PORT=59465
redis-server --port $PORT --protected-mode no &> redis.out &
REDIS=$!

echo "Redis started on $HOSTNAME:$PORT"

python synthetic.py \
    --funcx \
    --endpoint $ENDPOINT_UUID
    --redis-host $HOSTNAME \
    --redis-port $PORT \
    --input-size 1 \
    --output-size 0 \
    --interval 1 \
    --count 20 \
    --sleep-time 0 \
    --output-dir runs/funcx_1mb_inputs \
    --ps-redis
    --ps-threshold 0.01 \

# Kill the redis server
kill $REDIS

