import argparse
from pipeline_prototype.redis_q import ClientQueues


def cli_run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--param", required=True, type=float,
                        help="Parameter to pass into redis")
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("-q", "--qname", default="inputs",
                        help='Name of the redis-queue to send param to. Default: inputs')
    args = parser.parse_args()

    redis_queue = ClientQueues(args.redishost, port=args.redisport)

    if args.param == 'None':
        redis_queue.send_inputs(None)
    else:
        redis_queue.send_inputs(args.param)
    print(f"Pushed {args.param} to Redis Queue:{args.qname}")


if __name__ == "__main__":
    cli_run()
