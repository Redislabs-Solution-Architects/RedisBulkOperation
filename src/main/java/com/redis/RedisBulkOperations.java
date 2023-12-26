package com.redis;

import com.redis.operations.BulkOperation;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import javax.net.ssl.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RedisBulkOperations {

    private JedisPool pool;
    static public BlockingQueue< List<String> > eventQueue = null;
    private List<BulkOperation> operators = new ArrayList<BulkOperation>();
    public RedisBulkOperations(String configFile) {
        RBOConfig.getConfig().load(configFile);
        RBOConfig.getConfig().printConfig();
        try {
            connect();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (KeyManagementException e) {
            throw new RuntimeException(e);
        }

        eventQueue = new LinkedBlockingQueue<List<String>>();
    }

    public void run() throws InterruptedException {
        int total = 0;
        try ( Jedis jedis = pool.getResource() ) {
            long start = System.currentTimeMillis();
            if ( RBOConfig.getConfig().getOperation().equals("Count") && RBOConfig.getConfig().getPattern().equals("*") && !RBOConfig.getConfig().isVerbose()) {
                total = (int) jedis.dbSize();
            }
            else {
                addOperator();
                ScanResult<String> result;
                String cursor = ScanParams.SCAN_POINTER_START;
                 ScanParams scanParams = new ScanParams().match(RBOConfig.getConfig().getPattern()).count(RBOConfig.getConfig().getBulk());
                do {
                    result = jedis.scan(cursor, scanParams);
                    if (result.getResult().size() > 0) {
                        addBulk(result.getResult());
                    }
                    cursor = result.getCursor();
                } while (!result.getCursor().equals(ScanParams.SCAN_POINTER_START));
                System.out.println("Finished scanning keys in " + (System.currentTimeMillis() - start) / 1000 + " secs.");
                total = waitToFinish();
            }
            System.out.println("Finished operation on " + total + " keys in " + (System.currentTimeMillis() - start)/1000 + " secs.");

        }
    }

    private void connect() throws NoSuchAlgorithmException, KeyManagementException {
        JedisPoolConfig poolConfig=new JedisPoolConfig();
        poolConfig.setTestWhileIdle(false);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setNumTestsPerEvictionRun(-1);
        poolConfig.setMaxTotal(128);

        RBOConfig config = RBOConfig.getConfig();
        if ( config.isSSL() ) {
            TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {  }

                }
            };
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());

            final SSLSocketFactory sslSocketFactory = sc.getSocketFactory(); //(SSLSocketFactory) SSLSocketFactory.getDefault();
            final SSLParameters sslParameters = new SSLParameters();

            List<SNIServerName> sniServerNames = new ArrayList<SNIServerName>(1);
            if (!config.getSNI().isEmpty()) {
                sniServerNames.add(new SNIHostName(config.getSNI()));
                sslParameters.setServerNames(sniServerNames);
            }

            if (!config.getUser().isEmpty() && !config.getPassword().isEmpty())
                pool = new JedisPool(poolConfig, config.getHost(), config.getPort(), 10000, 10000, config.getUser(), config.getPassword(), 0, "", true, sslSocketFactory, sslParameters, null);
            else if (!config.getPassword().isEmpty())
                pool = new JedisPool(poolConfig, config.getHost(), config.getPort(), 10000, config.getPassword(), true, sslSocketFactory, sslParameters, null);
            else
                pool = new JedisPool(poolConfig, config.getHost(), config.getPort(), 10000, true, sslSocketFactory, sslParameters, null);
        }
        else {
            if (!config.getUser().isEmpty() && !config.getPassword().isEmpty())
                pool = new JedisPool(poolConfig, config.getHost(), config.getPort(), 10000, config.getUser(), config.getPassword(), false);
            if (!config.getPassword().isEmpty())
                pool = new JedisPool(poolConfig, config.getHost(), config.getPort(), 10000, config.getPassword(), false);
            else
                pool = new JedisPool(poolConfig, config.getHost(), config.getPort(), 10000, false);
        }
    }

    private int waitToFinish() {
        int total = 0;
        try {
            while (!eventQueue.isEmpty()) {
                Thread.sleep(100);
            }

            for (BulkOperation u: operators) {
                u.join();
                total += u.getCount();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        pool.close();
        return total;
    }

    private void addBulk(List<String> keys) throws InterruptedException {
        eventQueue.put(keys);
        if ( eventQueue.size() > 10 )
            addOperator();
    }

    private void addOperator()  {
        Class z = null;
        try {
            z = Class.forName("com.redis.operations." + "BulkOperation" + RBOConfig.getConfig().getOperation());
            Constructor<?> cons = z.getConstructor(JedisPool.class);
            BulkOperation operator = (BulkOperation) cons.newInstance(pool);
            operator.start();
            operators.add(operator);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Number of operators:" + operators.size());
    }

    public static void main(String[] args) {
        String configFile = "rbo.properties";
        if (args.length > 0)
            configFile = args[0];

        RedisBulkOperations rbo = new RedisBulkOperations(configFile);
        try {
            rbo.run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
