package com.redis.operations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;

public class BulkOperationCountNoTtl extends BulkOperation {

    final static String script = "local ttl = redis.call('ttl', KEYS[1])\n"
            + "if (ttl == -1) then\n"
            + "return 1\n"
            + "end\n"
            + "return 0";
    private String sha;
    public BulkOperationCountNoTtl(JedisPool pool) {
        super(pool);
        try ( Jedis jedis = pool.getResource() ) {
            sha = jedis.scriptLoad(script);
        }
    }

    @Override
    public int execute(List<String> keys) {
        long count = 0;
        try ( Jedis jedis = pool.getResource() ) {
            Pipeline p = jedis.pipelined();
            for ( String key: keys ) {
                p.evalsha(sha, 1, key);
            }
            List<Object> res = p.syncAndReturnAll();
            int i = 0;
            for ( Object o: res ) {
                count += (Long) o;
                if ((Long) o > 0)
                    printKey(keys.get(i));
                i++;
            }
        }
        return (int) count;
    }
}
