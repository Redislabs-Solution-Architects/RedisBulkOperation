package com.redis.operations;

import com.redis.RBOConfig;
import com.redis.RedisBulkOperations;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class BulkOperation extends Thread {

    public abstract int execute(List<String> keys);
    private boolean exit = false;
    protected JedisPool pool;

    protected int count = 0;
    protected boolean verbose = RBOConfig.getConfig().isVerbose();

    public BulkOperation(JedisPool pool) {
        this.pool = pool;
    }

    @Override
    public void run() {
        while (!exit) {
            List<String> keys = null;
            try {
                keys = RedisBulkOperations.eventQueue.poll(1000, TimeUnit.MILLISECONDS);
                if ( keys == null ) {
                    exit = true;
                    continue;
                }

                count += execute(keys);

            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    protected void printKeys(List<String> keys) {
        if ( verbose ) {
            for (String key : keys)
                System.out.println(key);
        }
    }

    protected void printKey(String key) {
        if ( verbose )
            System.out.println(key);
    }

    public int getCount() {
        return count;
    }
}
