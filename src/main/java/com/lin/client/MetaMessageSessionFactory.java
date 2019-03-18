package com.lin.client;

import com.lin.client.consumer.*;
import com.lin.client.consumer.storage.OffsetStorage;
import com.lin.client.consumer.storage.ZkOffsetStorage;
import com.lin.client.consumer.strategy.ConsisHashStrategy;
import com.lin.client.consumer.strategy.DefaultLoadBalanceStrategy;
import com.lin.client.consumer.strategy.LoadBalanceStrategy;
import com.lin.client.netty.RemoteClient;
import com.lin.client.netty.impl.DefaultRemoteClient;
import com.lin.client.producer.MessageProducer;
import com.lin.client.producer.ProducerZooKeeper;
import com.lin.client.producer.RoundRobinPartitionSelector;
import com.lin.client.producer.SimpleMessageProducer;
import com.lin.commons.PartitionSelector;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.InvalidConsumerConfigException;
import com.lin.commons.exception.InvalidOffsetStorageException;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.utils.ExceptionMonitor;
import com.lin.commons.utils.IdGenerator;
import com.lin.commons.utils.PropUtils;
import com.lin.commons.utils.ZkUtils;
import com.lin.commons.utils.zk.MetaZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 消息会话工厂，配置的优先级，优先使用传入的MetaClientConfig中的配置项，
 * 其次使用MetaClientConfig中的zkConfig配置的zk中的选项，如果都没有，则从diamond获取zk地址来获取配置项
 *
 * @author jianglinzou
 * @date 2019/3/11 下午1:16
 */
public class MetaMessageSessionFactory implements MessageSessionFactory {
    //最大重连时间，默认350s
    private static final int MAX_RECONNECT_TIMES = Integer.valueOf(System.getProperty(
            "metaq.client.network.max.reconnect.times", "350"));
    private static final int STATS_OPTIMEOUT = 3000;
    // TODO: 2019/3/9
//    protected RemotingClientWrapper remotingClient; //远程remotingClient,用于数据传输，相当于netty客户端
    protected RemoteClient remotingClient; //远程remotingClient,用于数据传输，相当于netty客户端
    private final MetaClientConfig metaClientConfig;//客户端配置
    private volatile ZkClient zkClient;//zk客户端

    static final Log log = LogFactory.getLog(MetaMessageSessionFactory.class);

    private final CopyOnWriteArrayList<ZkClientChangedListener> zkClientChangedListeners =
            new CopyOnWriteArrayList<ZkClientChangedListener>(); //监听器

    protected final ProducerZooKeeper producerZooKeeper; //producer与zookeeper

    private final ConsumerZooKeeper consumerZooKeeper; //consumer与zookeeper

    // private DiamondManager diamondManager; 由此工厂创建的所有子类，如消费者，生产者
    private final CopyOnWriteArrayList<Shutdownable> children = new CopyOnWriteArrayList<Shutdownable>();
    private volatile boolean shutdown; //是否关闭
    private volatile boolean isHutdownHookCalled = false;
    private final Thread shutdownHook;
    private ZkUtils.ZKConfig zkConfig; //zk配置
    private final RecoverManager recoverManager;
    private final SubscribeInfoManager subscribeInfoManager;

    protected final IdGenerator sessionIdGenerator; //id生成器，全局唯一

    protected MetaZookeeper metaZookeeper; //meta与zookeeper交互的辅助类

    //设置异常监视器实例
    static {
        ExceptionMonitor.setInstance(new ExceptionMonitor() {
            @Override
            public void exceptionCaught(final Throwable cause) {
                boolean isResetConnEx =
                        cause instanceof IOException && cause.getMessage().indexOf("Connection reset by peer") >= 0;
                if (log.isErrorEnabled() && !isResetConnEx) {
                    log.error("Networking unexpected exception", cause);
                }
            }
        });
    }


    /**
     * 返回通讯客户端
     *
     * @return
     */
//    public RemotingClientWrapper getRemotingClient() {
//        return this.remotingClient;
//    }


    /**
     * 返回订阅关系管理器
     *
     * @return
     */
//    public SubscribeInfoManager getSubscribeInfoManager() {
//        return this.subscribeInfoManager;
//    }


    /**
     * 返回客户端配置
     *
     * @return
     */
    public MetaClientConfig getMetaClientConfig() {
        return this.metaClientConfig;
    }


    /**
     * 返回生产者和zk交互管理器
     *
     * @return
     */
    public ProducerZooKeeper getProducerZooKeeper() {
        return this.producerZooKeeper;
    }


    /**
     * 返回消费者和zk交互管理器
     *
     * @return
     */
//    public ConsumerZooKeeper getConsumerZooKeeper() {
//        return this.consumerZooKeeper;
//    }


    /**
     * 返回本地恢复消息管理器
     *
     * @return
     */
//    public RecoverManager getRecoverStorageManager() {
//        return this.recoverManager;
//    }


    /**
     * 返回此工厂创建的所有子对象，如生产者、消费者等
     *
     * @return
     */
    public CopyOnWriteArrayList<Shutdownable> getChildren() {
        return this.children;
    }

    //tcp 连接属性
    public static final boolean TCP_NO_DELAY = Boolean.valueOf(System.getProperty("metaq.network.tcp_nodelay", "true"));
    public static final long MAX_SCHEDULE_WRITTEN_BYTES = Long.valueOf(System.getProperty(
            "metaq.network.max_schedule_written_bytes", String.valueOf(Runtime.getRuntime().maxMemory() / 3)));


    public MetaMessageSessionFactory(final MetaClientConfig metaClientConfig) throws SimpleMQClientException {
        super();
        try {
            this.checkConfig(metaClientConfig); //检查metaClientConfig配置
            this.metaClientConfig = metaClientConfig; //赋值
            //初始话通信端代码
//            final ClientConfig clientConfig = new ClientConfig(); //new 一个客户端配置，并赋值
//            clientConfig.setTcpNoDelay(TCP_NO_DELAY);
//            clientConfig.setMaxReconnectTimes(MAX_RECONNECT_TIMES);
//            clientConfig.setWireFormatType(new MetamorphosisWireFormatType());
//            clientConfig.setMaxScheduleWrittenBytes(MAX_SCHEDULE_WRITTEN_BYTES);

            //初始化通信客户端配置
            // TODO: 2019/3/12
            this.remotingClient = new DefaultRemoteClient();

            // 如果有设置，则使用设置的url并连接，否则使用zk发现服务器
            if (this.metaClientConfig.getServerUrl() != null) {
//                this.connectServer(this.metaClientConfig);
            } else {
                this.initZooKeeper();
            }

            this.producerZooKeeper =
                    new ProducerZooKeeper(this.metaZookeeper, this.remotingClient, this.zkClient, metaClientConfig);
            this.sessionIdGenerator = new IdGenerator();
            // modify by wuhua todo
            this.consumerZooKeeper = this.initConsumerZooKeeper(this.remotingClient, this.zkClient, this.zkConfig);
            this.zkClientChangedListeners.add(this.producerZooKeeper);
            // TODO: 2019/3/9
//            this.zkClientChangedListeners.add(this.consumerZooKeeper);
            this.subscribeInfoManager = new SubscribeInfoManager();
            this.recoverManager = new RecoverStorageManager(this.metaClientConfig, this.subscribeInfoManager);
            this.shutdownHook = new Thread() {

                @Override
                public void run() {
                    try {
                        MetaMessageSessionFactory.this.isHutdownHookCalled = true;
                        MetaMessageSessionFactory.this.shutdown();
                    } catch (final SimpleMQClientException e) {
                        log.error("关闭session factory失败", e);
                    }
                }

            };
            Runtime.getRuntime().addShutdownHook(this.shutdownHook);
        } catch (SimpleMQClientException e) {
            this.shutdown();
            throw e;
        } catch (Exception e) {
            this.shutdown();
            throw new SimpleMQClientException("Construct message session factory failed.", e);
        }
    }


    // add by wuhua
    protected ConsumerZooKeeper initConsumerZooKeeper(final RemoteClient remotingClientWrapper,
                                                      final ZkClient zkClient2, final ZkUtils.ZKConfig config) {
        return new ConsumerZooKeeper(this.metaZookeeper, this.remotingClient, this.zkClient, this.zkConfig);
    }


    private void checkConfig(final MetaClientConfig metaClientConfig) throws SimpleMQClientException {
        if (metaClientConfig == null) {
            throw new SimpleMQClientException("null configuration");
        }
    }


//    private void connectServer(final MetaClientConfig metaClientConfig) throws NetworkException {
//        try {
//            this.remotingClient.connect(metaClientConfig.getServerUrl());
//            this.remotingClient.awaitReadyInterrupt(metaClientConfig.getServerUrl());
//        }
//        catch (final NotifyRemotingException e) {
//            throw new NetworkException("Connect to " + metaClientConfig.getServerUrl() + " failed", e);
//        }
//        catch (final InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//    }


    private void initZooKeeper() throws SimpleMQClientException {
        // 优先使用设置的zookeepr，其次从diamond获取
        this.zkConfig = null;
        if (this.metaClientConfig.getZkConfig() != null) {
            this.zkConfig = this.metaClientConfig.getZkConfig();

        } else {
            this.zkConfig = this.loadZkConfigFromLocalFile();

        }
        if (this.zkConfig != null) {
            this.zkClient =
                    new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                            this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
            this.metaZookeeper = new MetaZookeeper(this.zkClient, this.zkConfig.zkRoot);
        } else {
            throw new SimpleMQClientException("No zk config offered");
        }
    }


    /*
     * (non-Javadoc)
     *
     * @see com.taobao.metamorphosis.client.SessionFactory#close()
     */

    public synchronized void shutdown() throws SimpleMQClientException {
        if (this.shutdown) {
            return;
        }
        this.shutdown = true;
        // if (this.diamondManager != null) {
        // this.diamondManager.close();
        // }
        //todo 关闭化恢复管理器
//        if (this.recoverManager != null) {
//            this.recoverManager.shutdown();
//        }
        // this.localMessageStorageManager.shutdown();
        for (final Shutdownable child : this.children) {
            if (child != null) {
                child.shutdown();
            }
        }
        //todo 关闭远程远程通信端
//        try {
//            if (this.remotingClient != null) {
//                this.remotingClient.stop();
//            }
//        } catch (final NotifyRemotingException e) {
//            throw new NetworkException("Stop remoting client failed", e);
//        }
        if (this.zkClient != null) {
            this.zkClient.close();
        }
        if (!this.isHutdownHookCalled && this.shutdownHook != null) {
            Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
        }

    }


    /**
     * 暂时从zk.properties里加载
     *
     * @return
     */
    // 单元测试要是通不过,请修改resources/zk.properties里的zk地址
    private ZkUtils.ZKConfig loadZkConfigFromLocalFile() {
        try {
            final Properties properties = PropUtils.getResourceAsProperties("zk.properties", "GBK");
            final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
            if (StringUtils.isNotBlank(properties.getProperty("zk.zkConnect"))) {
                zkConfig.zkConnect = properties.getProperty("zk.zkConnect");
            }

            if (StringUtils.isNotBlank(properties.getProperty("zk.zkSessionTimeoutMs"))) {
                zkConfig.zkSessionTimeoutMs = Integer.parseInt(properties.getProperty("zk.zkSessionTimeoutMs"));
            }

            if (StringUtils.isNotBlank(properties.getProperty("zk.zkConnectionTimeoutMs"))) {
                zkConfig.zkConnectionTimeoutMs = Integer.parseInt(properties.getProperty("zk.zkConnectionTimeoutMs"));
            }

            if (StringUtils.isNotBlank(properties.getProperty("zk.zkSyncTimeMs"))) {
                zkConfig.zkSyncTimeMs = Integer.parseInt(properties.getProperty("zk.zkSyncTimeMs"));
            }

            return zkConfig;// DiamondUtils.getZkConfig(this.diamondManager,
            // 10000);
        } catch (final IOException e) {
            log.error("zk配置失败", e);
            return null;
        }
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * com.taobao.metamorphosis.client.SessionFactory#createProducer(com.taobao
     * .metamorphosis.client.producer.PartitionSelector)
     */

    public MessageProducer createProducer(final PartitionSelector partitionSelector) {
        return this.createProducer(partitionSelector, false);
    }


    /*
     * (non-Javadoc)
     *
     * @see com.taobao.metamorphosis.client.SessionFactory#createProducer()
     */

    public MessageProducer createProducer() {
        return this.createProducer(new RoundRobinPartitionSelector(), false);
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * com.taobao.metamorphosis.client.SessionFactory#createProducer(boolean)
     */

    @Deprecated
    public MessageProducer createProducer(final boolean ordered) {
        return this.createProducer(new RoundRobinPartitionSelector(), ordered);
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * com.taobao.metamorphosis.client.SessionFactory#createProducer(com.taobao
     * .metamorphosis.client.producer.PartitionSelector, boolean)
     */
    @Deprecated
    public MessageProducer createProducer(final PartitionSelector partitionSelector, final boolean ordered) {
        if (partitionSelector == null) {
            throw new IllegalArgumentException("Null partitionSelector");
        }
        //todo 有待改善 全局唯一
        return this.addChild(new SimpleMessageProducer(this, this.remotingClient, partitionSelector,
                this.producerZooKeeper, System.currentTimeMillis()));
    }


    protected <T extends Shutdownable> T addChild(final T child) {
        this.children.add(child);
        return child;
    }


    /**
     * 删除子会话
     *
     * @param <T>
     * @param child
     */
    public <T extends Shutdownable> void removeChild(final T child) {
        this.children.remove(child);
    }


    private synchronized MessageConsumer createConsumer0(final ConsumerConfig consumerConfig,
                                                         final OffsetStorage offsetStorage, final RecoverManager recoverManager0) {
        if (consumerConfig.getServerUrl() == null) {
            consumerConfig.setServerUrl(this.metaClientConfig.getServerUrl());
        }
        if (offsetStorage == null) {
            throw new InvalidOffsetStorageException("Null offset storage");
        }
        // 必要时启动recover todo 恢复管理器有待后续完成
        if (!recoverManager0.isStarted()) {
            recoverManager0.start(this.metaClientConfig);
        }
        this.checkConsumerConfig(consumerConfig);
        return this.addChild(new SimpleMessageConsumer(this, this.remotingClient, consumerConfig,
                this.consumerZooKeeper, this.producerZooKeeper, this.subscribeInfoManager, recoverManager0, offsetStorage,
                this.createLoadBalanceStrategy(consumerConfig)));
    }


    protected LoadBalanceStrategy createLoadBalanceStrategy(final ConsumerConfig consumerConfig) {
        switch (consumerConfig.getLoadBalanceStrategyType()) {
            case DEFAULT:
                return new DefaultLoadBalanceStrategy();
            case CONSIST:
                return new ConsisHashStrategy();
            default:
                throw new IllegalArgumentException("Unknow load balance strategy type:"
                        + consumerConfig.getLoadBalanceStrategyType());
        }
    }


    protected MessageConsumer createConsumer(final ConsumerConfig consumerConfig, final OffsetStorage offsetStorage,
                                             final RecoverManager recoverManager0) {
        OffsetStorage offsetStorageCopy = offsetStorage;
        if (offsetStorageCopy == null) {
            // TODO: 2019/3/15
            offsetStorageCopy = new ZkOffsetStorage(this.metaZookeeper, this.zkClient);
            this.zkClientChangedListeners.add((ZkOffsetStorage) offsetStorageCopy);
        }

        return this.createConsumer0(consumerConfig, offsetStorageCopy, recoverManager0 != null ? recoverManager0
                : this.recoverManager);

    }


    @Override
    public MessageConsumer createConsumer(final ConsumerConfig consumerConfig, final OffsetStorage offsetStorage) {
        return this.createConsumer(consumerConfig, offsetStorage, this.recoverManager);
    }


//    @Override
//    public Map<InetSocketAddress, StatsResult> getStats(String item) throws InterruptedException {
//        return this.getStats0(null, item);
//    }


//    private Map<InetSocketAddress, StatsResult> getStats0(InetSocketAddress target, String item)
//            throws InterruptedException {
//        Set<String> groups = this.remotingClient.getGroupSet();
//        if (groups == null || groups.size() <= 1) {
//            return Collections.emptyMap();
//        }
//        Map<InetSocketAddress, StatsResult> rt = new HashMap<InetSocketAddress, StatsResult>();
//        try {
//            for (String group : groups) {
//                if (!group.equals(Constants.DEFAULT_GROUP)) {
//                    URI uri = new URI(group);
//                    InetSocketAddress sockAddr = new InetSocketAddress(uri.getHost(), uri.getPort());
//                    if (target == null || target.equals(sockAddr)) {
//                        BooleanCommand resp =
//                                (BooleanCommand) this.remotingClient.invokeToGroup(group, new StatsCommand(
//                                        OpaqueGenerator.getNextOpaque(), item), STATS_OPTIMEOUT, TimeUnit.MILLISECONDS);
//                        if (resp.getResponseStatus() == ResponseStatus.NO_ERROR) {
//                            String body = resp.getErrorMsg();
//                            if (body != null) {
//                                this.parseStatsValues(sockAddr, rt, group, body);
//                            }
//                        }
//                    }
//                }
//            }
//            return rt;
//        } catch (InterruptedException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new IllegalStateException("Get statistics from brokers failed", e);
//        }
//    }


    public List<Partition> getPartitionsForTopic(String topic) {
        if (this.metaZookeeper != null) {
            List<String> topics = new ArrayList<String>(1);
            topics.add(topic);
            List<Partition> rt = this.metaZookeeper.getPartitionsForTopicsFromMaster(topics).get(topic);
            if (rt == null) {
                return Collections.emptyList();
            } else {
                return rt;
            }
        } else {
            throw new IllegalStateException("Could not talk with zookeeper to get partitions list");
        }
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * com.taobao.metamorphosis.client.SessionFactory#createConsumer(com.taobao
     * .metamorphosis.client.consumer.ConsumerConfig)
     */
//    @Override
    public MessageConsumer createConsumer(final ConsumerConfig consumerConfig) {
        return this.createConsumer(consumerConfig, null, null);
    }

    static final char[] INVALID_GROUP_CHAR = {'~', '!', '#', '$', '%', '^', '&', '*', '(', ')', '+', '=', '`', '\'',
            '"', ',', ';', '/', '?', '[', ']', '<', '>', '.', ':', ' '};


    protected void checkConsumerConfig(final ConsumerConfig consumerConfig) {
        if (StringUtils.isBlank(consumerConfig.getGroup())) {
            throw new InvalidConsumerConfigException("Blank group");
        }
        final char[] chary = new char[consumerConfig.getGroup().length()];
        consumerConfig.getGroup().getChars(0, chary.length, chary, 0);
        for (final char ch : chary) {
            for (final char invalid : INVALID_GROUP_CHAR) {
                if (ch == invalid) {
                    throw new InvalidConsumerConfigException("Group name has invalid character " + ch);
                }
            }
        }
        if (consumerConfig.getFetchRunnerCount() <= 0) {
            throw new InvalidConsumerConfigException("Invalid fetchRunnerCount:" + consumerConfig.getFetchRunnerCount());
        }
        if (consumerConfig.getFetchTimeoutInMills() <= 0) {
            throw new InvalidConsumerConfigException("Invalid fetchTimeoutInMills:"
                    + consumerConfig.getFetchTimeoutInMills());
        }
    }


//    @Override
//    public TopicBrowser createTopicBrowser(String topic) {
//        return this.createTopicBrowser(topic, 1024 * 1024, 5, TimeUnit.SECONDS);
//    }

//
//    @Override
//    public TopicBrowser createTopicBrowser(String topic, int maxSize, long timeout, TimeUnit timeUnit) {
//        MessageConsumer consumer = this.createConsumer(new ConsumerConfig("Just_for_Browser"));
//        return new MetaTopicBrowser(topic, maxSize, TimeUnit.MILLISECONDS.convert(timeout, timeUnit), consumer,
//                this.getPartitionsForTopic(topic));
//    }

}
