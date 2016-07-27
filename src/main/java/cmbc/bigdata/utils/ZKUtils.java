package cmbc.bigdata.utils;

import cmbc.bigdata.constants.CONSTANTSUTIL;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangpengcheng on 2016/7/21 0021.
 */

public enum ZKUtils{
    INSTANCE;

    /**
     * 初始sleep时间(毫秒)
     */
    private static final int BASE_SLEEP_TIME = 1000;
    /**
     * 最大重试次数
     */
    private static final int MAX_RETRIES_COUNT = 5;
    /**
     * 最大sleep时间
     */
    private static final int MAX_SLEEP_TIME = 60000;

    private static final String CONNECT_STRING = "192.168.145.129:2181,192.168.145.130:2181,192.168.145.131:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private static final int CONNECTION_TIMEOUT = 5000;
    private CuratorFramework client=null;

    ZKUtils(){

        //1.设置重试策略,重试时间计算策略sleepMs = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)));
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME, MAX_RETRIES_COUNT, MAX_SLEEP_TIME);

        //2.初始化客户端
        client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_STRING)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .connectionTimeoutMs(CONNECTION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .namespace(CONSTANTSUTIL.DEFAULT_NS)		//命名空间隔离
                .build();
        client.start();
        try {
            client.blockUntilConnected();
            Logger logger = LoggerFactory.getLogger(ZKUtils.class);
            logger.info("Zookeeper:" + CONNECT_STRING + " Connected.Continue...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public CuratorFramework getClient(){
        return client;
    }
}

