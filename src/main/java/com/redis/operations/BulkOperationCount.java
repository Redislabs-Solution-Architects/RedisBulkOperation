package com.redis.operations;

import redis.clients.jedis.JedisPool;

import java.util.List;

public class BulkOperationCount extends BulkOperation {

    public BulkOperationCount(JedisPool pool) {
        super(pool);
    }

    @Override
    public int execute(List<String> keys) {
        printKeys(keys);
        return keys.size();
    }
}
