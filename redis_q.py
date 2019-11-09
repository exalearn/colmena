import json
import redis

class RedisQueue(object):
    """ A basic redis queue

    The queue only connects when the `connect` method is called to avoid
    issues with passing an object across processes.

    Parameters
    ----------

    hostname : str
       Hostname of the redis server

    port : int
       Port at which the redis server can be reached. Default: 6379

    """

    def __init__(self, hostname, port=6379, prefix='pipeline'):
        """ Initialize
        """
        self.hostname = hostname
        self.port = port
        self.redis_client = None
        self.prefix = prefix

    def connect(self):
        """ Connects to the Redis server
        """
        try:
            if not self.redis_client:
                self.redis_client = redis.StrictRedis(host=self.hostname, port=self.port, decode_responses=True)
        except redis.exceptions.ConnectionError:
            print("ConnectionError while trying to connect to Redis@{}:{}".format(self.hostname,
                                                                                  self.port))

            raise

    def get(self, timeout=None):
        """ Get an item from the redis queue

        Parameters
        ----------
        timeout : int
           Timeout for the blocking get in seconds
        """
        params = None
        try:
            if timeout == None:
                result = self.redis_client.blpop(self.prefix)
            else:
                result = self.redis_client.blpop(self.prefix, timeout=int(timeout))

            if result == None:
                params == None
            else:
                q, js = result
                print("Got from pop : ", js)
                params = js

        except AttributeError:
            raise Exception("Queue is empty/flushed")
        except redis.exceptions.ConnectionError:
            print(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

        return params

    def put(self, params):
        """ Put's the key:payload into a dict and pushes the key onto a queue
        Parameters
        ----------
        key : str
            The task_id to be pushed

        payload : dict
            Dict of task information to be stored
        """
        try:
            js = json.dumps(params)
            self.redis_client.rpush(self.prefix, js)
        except AttributeError:
            raise NotConnected(self)
        except redis.exceptions.ConnectionError:
            print("ConnectionError while trying to connect to Redis@{}:{}".format(self.hostname,
                                                                                  self.port))
            raise

    def flush(self):
        """ Flush the REDIS queue
        """
        try:
            self.redis_client.delete(self.prefix)
        except AttributeError:
            raise Exception("Queue is empty/flushed")
        except redis.exceptions.ConnectionError:
            print("ConnectionError while trying to connect to Redis@{}:{}".format(self.hostname,
                                                                                  self.port))
            raise

    @property
    def is_connected(self):
        return self.redis_client is not None


if __name__ == "__main__":

    rq = RedisQueue('127.0.0.1')
    rq.connect()
    #    rq.flush()
    import random
    rq.put(random.randint(0, 100))
    x = rq.get()
    print("Result : ", x)
