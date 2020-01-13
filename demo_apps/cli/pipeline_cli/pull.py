import argparse
from pipeline_prototype.redis_q import ClientQueues


def cli_run():

    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("-t", "--timeout", default=None,
                        help="Timeout for Redis request")
    args = parser.parse_args()

    redis_queue = ClientQueues(args.redishost, port=args.redisport)

    value = redis_queue.get_result(timeout=args.timeout)
    print(f"Pulled from Redis: {value.value}")


if __name__ == "__main__":
    cli_run()
