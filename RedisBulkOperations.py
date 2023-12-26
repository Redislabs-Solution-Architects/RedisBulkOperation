import argparse
import redis
import datetime
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty

from threading import Thread, Lock

queue = Queue()
lock = Lock()


def safe_print(key):
    with lock:
        print(key)


deleteLua = "local ttl = redis.call('ttl', KEYS[1])\n"\
            "if (ttl == -1) then\n"\
            "redis.call('unlink', KEYS[1])\n"\
            "return 1\n"\
            "end\n"\
            "return 0";

ttlLua = "local ttl = redis.call('ttl', KEYS[1])\n"\
            "local setTtl = tonumber(ARGV[1])\n"\
            "if (ttl == -1) then\n"\
            "redis.call('expire', KEYS[1], setTtl)\n"\
            "return 1\n"\
            "end\n"\
            "return 0";

ttlLTLua = "local ttl = redis.call('ttl', KEYS[1])\n"\
            "local setTtl = tonumber(ARGV[1])\n"\
            "if (ttl < setTtl) then\n"\
            "redis.call('expire', KEYS[1], setTtl)\n"\
            "return 1\n"\
            "end\n"\
            "return 0";

class Operator(Thread):
    def __init__(self, operation, redis, verbose, ttl):
        Thread.__init__(self)
        self.count = 0
        self.exit = False
        self.redis = redis
        self.verbose = verbose
        self.ttl = ttl
        self.func = getattr(Operator, operation)
        self.loadLua(operation)


    def run(self):
        while self.exit == False:
            try:
                keys = queue.get(True, 1)
            except Empty:
                self.exit = True
                continue

            self.count += self.func(self, keys)

    def loadLua(self, operation):
        if operation == 'deleteNoTtl':
            self.sha = self.redis.script_load(deleteLua)
        elif operation == 'setTtl':
            self.sha = self.redis.script_load(ttlLua)
        elif operation == 'setTtlLessThan':
            self.sha = self.redis.script_load(ttlLTLua)

    def count(self, keys):
        if self.verbose:
            for k in keys:
                safe_print(k)
        return len(keys)

    def countNoTtl(self, keys):
        count = 0
        p = self.redis.pipeline(transaction=False)
        for k in keys:
            p.ttl(k)
        i = 0
        res = p.execute()
        for r in res:
            if r == -1:
                count += 1
                if self.verbose:
                    safe_print(keys[i])
            i += 1

        return count

    def countTtl(self, keys):
        count = 0
        p = self.redis.pipeline(transaction=False)
        for k in keys:
            p.ttl(k)
        res = p.execute()
        i = 0
        for r in res:
            if r > -1:
                count += 1
                if self.verbose:
                    safe_print(keys[i])
            i += 1

        return count

    def countTtlLessThan(self, keys):
        count = 0
        p = self.redis.pipeline(transaction=False)
        for k in keys:
            p.ttl(k)
        res = p.execute()
        i = 0
        for r in res:
            if r > self.ttl:
                count += 1
                if self.verbose:
                    safe_print(keys[i])
            i += 1

        return count

    def delete(self, keys):
        p = self.redis.pipeline(transaction=False)
        for k in keys:
            p.unlink(k)
            if self.verbose:
                safe_print(k)
        p.execute()

        return len(keys)

    def deleteNoTtl(self, keys):
        count = 0
        p = self.redis.pipeline(transaction=False)
        for k in keys:
            p.evalsha(self.sha, 1, k)
        res = p.execute()
        i = 0
        for r in res:
            count += r
            if self.verbose and r > 0:
                safe_print(keys[i])
            i += 1

        return count

    def setTtl(self, keys):
        count = 0
        p = self.redis.pipeline(transaction=False)
        for k in keys:
            p.evalsha(self.sha, 1, k, str(self.ttl))
        res = p.execute()
        i = 0
        for r in res:
            count += r
            if self.verbose and r > 0:
                safe_print(keys[i])
            i += 1

        return count

        return count

    def setTtlLessThan(self, keys):
        count = 0
        p = self.redis.pipeline(transaction=False)
        for k in keys:
            p.evalsha(self.sha, 1, k, str(self.ttl))
        res = p.execute()
        i = 0
        for r in res:
            count += r
            if self.verbose and r > 0:
                safe_print(keys[i])
            i += 1

        return count

    def getCount(self):
        return self.count

class BulkOperation:
    def __init__(self, args):
        self.operation=args.operation
        self.pattern = args.pattern
        self.ttl = args.ttl
        self.bulk = args.bulk
        self.operators = []
        self.verbose = args.verbose
        self.redis = redis.Redis(args.host, args.port,
                                 username=args.user,
                                 password=args.password,
                                 ssl=args.ssl,
                                 ssl_keyfile=args.key,
                                 ssl_certfile=args.cert,
                                 ssl_cert_reqs='required',
                                 ssl_ca_certs=args.ca)

    def process(self):
        startt = datetime.datetime.now()
        if self.operation == 'count' and self.pattern == '*' and self.verbose is False:
            s = self.redis.dbsize()
        else:
            self.addOperator(self.operation)
            self.scanKeys(startt)
            s = self.waitToFinish()
        delta = datetime.datetime.now() - startt
        print("Finished operation on " + str(s) + " keys in " + str(int(delta.total_seconds())) +  " seconds.")

    def scanKeys(self, startt):
        cursor = 0
        ret = self.redis.scan(cursor=cursor, match=self.pattern, count=self.bulk)
        keys = ret[1]
        cursor = ret[0]
        self.addBulk(keys)
        while cursor > 0:
            ret = self.redis.scan(cursor=cursor, match=self.pattern, count=self.bulk)
            keys = ret[1]
            self.addBulk(keys)
            cursor = ret[0]

        delta = datetime.datetime.now() - startt
        print("Finished scanning keys in " + str(int(delta.total_seconds())) + " secs.");

    def addBulk(self, keys):
        queue.put(keys)
        if queue.qsize() > 10:
            self.addOperator(self.operation)

    def addOperator(self, operation):
        operator = Operator(operation, self.redis, self.verbose, self.ttl)
        operator.start()
        self.operators.append(operator)
        print("number of operators: " + str(len(self.operators)))

    def waitToFinish(self):
        total = 0
        for o in self.operators:
            o.join()
            total += o.getCount()

        return total

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__)
    parser.add_argument(
        '-s, --host', dest='host', type=str, help='Server address',
        default='localhost')
    parser.add_argument(
        '-p, --port', dest='port', type=int, help='Server port',
        default=6379)
    parser.add_argument(
        '-u, --user', dest='user', type=str, help='User name', default="")
    parser.add_argument(
        '-w, --password', dest='password', type=str, help='Password', default="")
    parser.add_argument(
        '-o, --operation', dest='operation', type=str, help='Operation: count|countNoTtl|countTtl|delete|deleteNoTtl|setTll|setTtlLessThan', default="count")
    parser.add_argument(
        '-k, --key-pattern', dest='pattern', type=str, help='Key pattern', default="*")
    parser.add_argument(
        '-t, --ttl', dest='ttl', type=int, help='TTL in seconds')
    parser.add_argument(
        '-b, --bulk', dest='bulk', type=int, help='Bulk size',
        default=1000)
    parser.add_argument('-v, --verbose', dest='verbose', action='store_true', help='Print keys')
    parser.add_argument(
        '-l, --ssl', dest='ssl', action='store_true', help='SSL connection')
    parser.add_argument(
        '--key-file', dest='key', type=str, help='Private key file')
    parser.add_argument(
        '--cert-file', dest='cert', type=str, help='Certificate file')
    parser.add_argument(
        '--ca-file', dest='ca', type=str, help='CA file')

    args = parser.parse_args()
    if (args.operation == 'setTtl' or args.operation == 'setTtlLessThan') and args.ttl is None:
        print('Missing parameter ttl')
        exit(-1)
    print(args)
    bulkOp = BulkOperation(args)
    bulkOp.process()


if __name__ == '__main__':
    main()
