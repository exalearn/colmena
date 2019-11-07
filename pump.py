import argparse
from redis_q import RedisQueue

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--param", required=True,
                        help="Parameter to pass into redis")
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    args = parser.parse_args()

    redis_queue = RedisQueue(args.redishost, port=int(args.redisport))
    redis_queue.connect()

    redis_queue.put(int(args.param))
    print(f"Pushed {args.param} to Redis")
