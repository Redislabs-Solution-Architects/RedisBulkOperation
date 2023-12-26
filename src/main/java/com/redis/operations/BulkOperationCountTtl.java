package com.redis.operations;

import com.redis.RBOConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;

public class BulkOperationCountTtl extends BulkOperation {

    final static String script = "local ttl = redis.call('ttl', KEYS[1])\n"
            + "local minTtl = tonumber(ARGV[1])\n"
            + "if (ttl > minTtl) then\n"
            + "return 1\n"
            + "end\n"
            + "return 0";
    private String sha;
    private int ttl;
    public BulkOperationCountTtl(JedisPool pool) {
        super(pool);
        try ( Jedis jedis = pool.getResource() ) {
            sha = jedis.scriptLoad(script);
        }

        ttl = Integer.parseInt(RBOConfig.getConfig().getProperty("ttl"));
    }

    @Override
    public int execute(List<String> keys) {
        long count = 0;
        try ( Jedis jedis = pool.getResource() ) {
            Pipeline p = jedis.pipelined();
            for ( String key: keys ) {
                p.evalsha(sha, 1, key, "" + ttl);
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
