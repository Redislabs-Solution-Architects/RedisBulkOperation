package com.redis.operations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;

public class BulkOperationDelete extends BulkOperation {

    public BulkOperationDelete(JedisPool pool) {
        super(pool);
    }

    @Override
    public int execute(List<String> keys) {
        try ( Jedis jedis = pool.getResource() ) {
            Pipeline p = jedis.pipelined();
            for ( String key: keys ) {
                p.unlink(key);
                printKey(key);
            }
            p.sync();
        }
        return keys.size();
    }
}
