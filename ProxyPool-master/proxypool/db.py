import redis
from proxypool.error import PoolEmptyError
from proxypool.setting import HOST, PORT, PASSWORD


class RedisClient(object):
    def __init__(self, host=HOST, port=PORT):
        if PASSWORD:
            self._db = redis.Redis(host=host, port=port, password=PASSWORD)
        else:
            self._db = redis.Redis(host=host, port=port)

    def get(self, count=1):
        """
        get proxies from redis
        """
        proxies = self._db.lrange("proxies", 0, count - 1) #从队列的左侧拿出多少个进行检测，保证最新的代理都是可用的
        self._db.ltrim("proxies", count, -1)
        return proxies

    def put(self, proxy):
        """
        add proxy to right top
        """
        self._db.rpush("proxies", proxy) #检测后新的代理放入

    def pop(self):
        """
        get proxy from right.
        """
        try:
            return self._db.rpop("proxies").decode('utf-8')  #取出最新的代理
        except:
            raise PoolEmptyError

    @property
    def queue_len(self):
        """
        get length from queue.
        """
        return self._db.llen("proxies")

    def flush(self):
        """
        flush db
        """
        self._db.flushall()


if __name__ == '__main__':
    conn = RedisClient()
    print(conn.pop())
