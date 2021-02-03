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
port=59465
#63${RANDOM::2}
redis-server --port $port --protected-mode no &> redis.out &
redis=$!

echo "Redis started on $HOSTNAME:$port"

python synthetic.py --config $CONFIG --redis-host $HOSTNAME --redis-port $port

# Kill the redis server
kill $redis

