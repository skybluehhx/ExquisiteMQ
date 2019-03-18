package com.lin.client.consumer;

import com.lin.client.consumer.storage.OffsetStorage;
import com.lin.client.consumer.strategy.LoadBalanceStrategy;
import com.lin.client.netty.RemoteClient;
import com.lin.commons.cluster.Broker;
import com.lin.commons.cluster.Cluster;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.NotifyRemotingException;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.utils.ThreadUtils;
import com.lin.commons.utils.ZkUtils;
import com.lin.commons.utils.network.RemotingUtils;
import com.lin.commons.utils.zk.MetaZookeeper;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Consumer与Zookeeper交互
 *
 * @author jianglinzou
 * @date 2019/3/11 下午1:23
 */
public class ConsumerZooKeeper implements ZkClientChangedListener {

    protected ZkClient zkClient;
    protected final ConcurrentHashMap<FetchManager, FutureTask<ZKLoadRebalanceListener>> consumerLoadBalanceListeners =
            new ConcurrentHashMap<FetchManager, FutureTask<ZKLoadRebalanceListener>>();
    private final RemoteClient remotingClient;
    private final ZkUtils.ZKConfig zkConfig; //保存连接zk的配置
    protected final MetaZookeeper metaZookeeper; //与zk交接的辅助类


    public ConsumerZooKeeper(final MetaZookeeper metaZookeeper, final RemoteClient remotingClient,
                             final ZkClient zkClient, final ZkUtils.ZKConfig zkConfig) {
        super();
        this.metaZookeeper = metaZookeeper;
        this.zkClient = zkClient;
        this.remotingClient = remotingClient;
        this.zkConfig = zkConfig;
    }


    public void commitOffsets(final FetchManager fetchManager) {
        final ZKLoadRebalanceListener listener = this.getBrokerConnectionListener(fetchManager);
        if (listener != null) {
            listener.commitOffsets();
        }
    }


    public ZKLoadRebalanceListener getBrokerConnectionListener(final FetchManager fetchManager) {
        final FutureTask<ZKLoadRebalanceListener> task = this.consumerLoadBalanceListeners.get(fetchManager);
        if (task != null) {
            try {
                return task.get();
            } catch (final ExecutionException e) {
                throw ThreadUtils.launderThrowable(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }


    /**
     * 取消注册consumer
     *
     * @param fetchManager
     */
    public void unRegisterConsumer(final FetchManager fetchManager) {
        try {
            final FutureTask<ZKLoadRebalanceListener> futureTask =
                    this.consumerLoadBalanceListeners.remove(fetchManager);
            if (futureTask != null) {
                final ZKLoadRebalanceListener listener = futureTask.get();
                if (listener != null) {
                    listener.stop();
                    // 提交offsets
                    listener.commitOffsets();
                    this.zkClient.unsubscribeStateChanges(new ZKSessionExpireListenner(listener));
                    final MetaZookeeper.ZKGroupDirs dirs = this.metaZookeeper.new ZKGroupDirs(listener.consumerConfig.getGroup());
                    this.zkClient.unsubscribeChildChanges(dirs.consumerRegistryDir, listener);
                    log.info("unsubscribeChildChanges:" + dirs.consumerRegistryDir);
                    // 移除监视订阅topic的分区变化
                    for (final String topic : listener.topicSubcriberRegistry.keySet()) {
                        final String partitionPath = this.metaZookeeper.brokerTopicsSubPath + "/" + topic;
                        this.zkClient.unsubscribeChildChanges(partitionPath, listener);
                        log.info("unsubscribeChildChanges:" + partitionPath);
                    }
                    // 删除ownership
                    listener.releaseAllPartitionOwnership();
                    // 删除临时节点
                    ZkUtils.deletePath(this.zkClient, listener.dirs.consumerRegistryDir + "/"
                            + listener.consumerIdString);

                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted when unRegisterConsumer", e);
        } catch (final Exception e) {
            log.error("Error in unRegisterConsumer,maybe error when registerConsumer", e);
        }
    }


    /**
     * 注册订阅者
     *
     * @throws Exception
     */
    public void registerConsumer(final ConsumerConfig consumerConfig, final FetchManager fetchManager,
                                 final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry,
                                 final OffsetStorage offsetStorage, final LoadBalanceStrategy loadBalanceStrategy) throws Exception {

        final FutureTask<ZKLoadRebalanceListener> task =
                new FutureTask<ZKLoadRebalanceListener>(new Callable<ZKLoadRebalanceListener>() {


                    public ZKLoadRebalanceListener call() throws Exception {
                        //meta/consumers/group(具体的组名)/ids
                        final MetaZookeeper.ZKGroupDirs dirs =
                                ConsumerZooKeeper.this.metaZookeeper.new ZKGroupDirs(consumerConfig.getGroup());
                        //根据配置生成consumerUUID如果没有配置则由系统自动生成，建议设置
                        final String consumerUUID = ConsumerZooKeeper.this.getConsumerUUID(consumerConfig);
                        final String consumerUUIDString = consumerConfig.getGroup() + "_" + consumerUUID;
                        //添加负载均衡监听器；
                        final ZKLoadRebalanceListener loadBalanceListener =
                                new ZKLoadRebalanceListener(fetchManager, dirs, consumerUUIDString, consumerConfig,
                                        offsetStorage, topicSubcriberRegistry, loadBalanceStrategy);
                        loadBalanceListener.start();
                        return ConsumerZooKeeper.this.registerConsumerInternal(loadBalanceListener);
                    }

                });
        final FutureTask<ZKLoadRebalanceListener> existsTask =
                this.consumerLoadBalanceListeners.putIfAbsent(fetchManager, task);
        if (existsTask == null) {
            task.run();
        } else {
            throw new SimpleMQClientException("Consumer has been already registed");
        }

    }


    protected ZKLoadRebalanceListener registerConsumerInternal(final ZKLoadRebalanceListener loadBalanceListener)
            throws UnknownHostException, InterruptedException, Exception {
        final MetaZookeeper.ZKGroupDirs dirs = this.metaZookeeper.new ZKGroupDirs(loadBalanceListener.consumerConfig.getGroup());

        final String topicString = this.getTopicsString(loadBalanceListener.topicSubcriberRegistry);

        if (this.zkClient == null) { //不使用zk
            // 直连模式
            loadBalanceListener.fetchManager.stopFetchRunner();
            loadBalanceListener.fetchManager.resetFetchState();
            // zkClient为null，使用配置项并发起fetch请求
            for (final String topic : loadBalanceListener.topicSubcriberRegistry.keySet()) {
                final SubscriberInfo subInfo = loadBalanceListener.topicSubcriberRegistry.get(topic);
                ConcurrentHashMap<Partition, TopicPartitionRegInfo> topicPartRegInfoMap =
                        loadBalanceListener.topicRegistry.get(topic);
                if (topicPartRegInfoMap == null) {
                    topicPartRegInfoMap = new ConcurrentHashMap<Partition, TopicPartitionRegInfo>();
                    loadBalanceListener.topicRegistry.put(topic, topicPartRegInfoMap);
                }
                final Partition partition = new Partition(loadBalanceListener.consumerConfig.getPartition());
                long offset = loadBalanceListener.consumerConfig.getOffset();
                if (loadBalanceListener.consumerConfig.isAlwaysConsumeFromMaxOffset()) {
                    offset = Long.MAX_VALUE;
                }
                final TopicPartitionRegInfo regInfo = new TopicPartitionRegInfo(topic, partition, offset);
                topicPartRegInfoMap.put(partition, regInfo);
                loadBalanceListener.fetchManager.addFetchRequest(new FetchRequest(new Broker(0,
                        loadBalanceListener.consumerConfig.getServerUrl()), 0L, regInfo, subInfo.getMaxSize()));
            }
            loadBalanceListener.fetchManager.startFetchRunner();
        } else {  //使用zk
            for (int i = 0; i < MAX_N_RETRIES; i++) {
                // 注册consumer id  meta/consumers/group/ids/consumerIdString :data(topicString)
                ZkUtils.makeSurePersistentPathExists(this.zkClient, dirs.consumerRegistryDir);
                ZkUtils.createEphemeralPathExpectConflict(this.zkClient, dirs.consumerRegistryDir + "/"
                        + loadBalanceListener.consumerIdString, topicString);
                // 监视同一个分组的consumer列表是否有变化
                this.zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalanceListener);

                // 监视订阅topic的分区是否有变化,/brokers/topics-sub/topic(具体主题)/
                for (final String topic : loadBalanceListener.topicSubcriberRegistry.keySet()) {
                    // /brokers/topics-sub/topic(具体主题)
                    final String partitionPath = this.metaZookeeper.brokerTopicsSubPath + "/" + topic;
                    ZkUtils.makeSurePersistentPathExists(this.zkClient, partitionPath);
                    this.zkClient.subscribeChildChanges(partitionPath, loadBalanceListener);
                }

                // 监视zk client状态，在连接重连的时候重新注册
                this.zkClient.subscribeStateChanges(new ZKSessionExpireListenner(loadBalanceListener));

                // 第一次，需要明确触发balance
                if (loadBalanceListener.syncedRebalance()) {
                    break;
                }
            }
        }
        return loadBalanceListener;
    }


    private String getTopicsString(final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry) {
        final StringBuilder topicSb = new StringBuilder();
        boolean wasFirst = true;
        for (final String topic : topicSubcriberRegistry.keySet()) {
            if (wasFirst) {
                wasFirst = false;
                topicSb.append(topic);
            } else {
                topicSb.append(",").append(topic);
            }
        }
        return topicSb.toString();
    }


    protected String getConsumerUUID(final ConsumerConfig consumerConfig) throws Exception {
        String consumerUUID = null;
        if (consumerConfig.getConsumerId() != null) {
            consumerUUID = consumerConfig.getConsumerId();
        } else {
            consumerUUID =
                    RemotingUtils.getLocalHost() + "-" + this.getPid() + "-" + UUID.randomUUID();
        }
        return consumerUUID;
    }


    private String getPid() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();
        if (name.contains("@")) {
            return name.split("@")[0];
        }
        return name;
    }


    public void onZkClientChanged(final ZkClient newClient) {
        this.zkClient = newClient;
        // 重新注册consumer
        for (final FutureTask<ZKLoadRebalanceListener> task : this.consumerLoadBalanceListeners.values()) {
            try {
                final ZKLoadRebalanceListener listener = task.get();
                // 要清空已有的注册信息，防止在注册consumer失败的时候还提交offset，导致覆盖更新的offset
                listener.topicRegistry.clear();
                log.info("re-register consumer to zk,group=" + listener.consumerConfig.getGroup());
                this.registerConsumerInternal(listener);
            } catch (final Exception e) {
                log.error("reRegister consumer failed", e);
            }
        }

    }

    class ZKSessionExpireListenner implements IZkStateListener {
        private final String consumerIdString;
        private final ZKLoadRebalanceListener loadBalancerListener;


        public ZKSessionExpireListenner(final ZKLoadRebalanceListener loadBalancerListener) {
            super();
            this.consumerIdString = loadBalancerListener.consumerIdString;
            this.loadBalancerListener = loadBalancerListener;
        }


        public void handleNewSession() throws Exception {
            /**
             * When we get a SessionExpired event, we lost all ephemeral nodes
             * and zkclient has reestablished a connection for us. We need to
             * release the ownership of the current consumer and re-register
             * this consumer in the consumer registry and trigger a rebalance.
             */
            ;
            log.info("ZK expired; release old broker parition ownership; re-register consumer " + this.consumerIdString);
            this.loadBalancerListener.resetState();
            ConsumerZooKeeper.this.registerConsumerInternal(this.loadBalancerListener);
        }


        public void handleStateChanged(final Watcher.Event.KeeperState state) throws Exception {
            // do nothing, since zkclient will do reconnect for us.

        }


        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ZKSessionExpireListenner)) {
                return false;
            }
            final ZKSessionExpireListenner other = (ZKSessionExpireListenner) obj;
            return this.loadBalancerListener.equals(other.loadBalancerListener);
        }


        @Override
        public int hashCode() {
            return this.loadBalancerListener.hashCode();
        }

    }

    static final int MAX_N_RETRIES = 7;

    static final Log log = LogFactory.getLog(ConsumerZooKeeper.class);

    /**
     * Consumer load balance listener for zookeeper. This is a internal class
     * for consumer,you should not use it directly in your code.
     *
     * @author dennis<killme2008                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               @                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               gmail.com>
     */
    public class ZKLoadRebalanceListener implements IZkChildListener, Runnable {
        private final MetaZookeeper.ZKGroupDirs dirs;

        private final String group;

        protected final String consumerIdString;

        private final LoadBalanceStrategy loadBalanceStrategy;

        Map<String, List<String>> oldConsumersPerTopicMap = new HashMap<String, List<String>>();

        Map<String, List<String>> oldPartitionsPerTopicMap = new HashMap<String, List<String>>();

        private final Lock rebalanceLock = new ReentrantLock();

        /**
         * 该消费者 订阅的topic对应的broker,offset等信息
         */
        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                new ConcurrentHashMap<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>>();

        /**
         * 订阅信息，如最大传输大小，消息监听器等，该消费者订阅的每个主题的信息
         */
        private final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry;

        private final ConsumerConfig consumerConfig;

        private final OffsetStorage offsetStorage;

        private final FetchManager fetchManager;

        private final Thread rebalanceThread;

        private volatile boolean stopped = false;

        Set<Broker> oldBrokerSet = new HashSet<Broker>();
        private Cluster oldCluster = new Cluster();


        public ZKLoadRebalanceListener(final FetchManager fetchManager, final MetaZookeeper.ZKGroupDirs dirs,
                                       final String consumerIdString, final ConsumerConfig consumerConfig, final OffsetStorage offsetStorage,
                                       final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry,
                                       final LoadBalanceStrategy loadBalanceStrategy) {
            super();
            this.fetchManager = fetchManager;
            this.dirs = dirs;
            this.consumerIdString = consumerIdString;
            this.group = consumerConfig.getGroup();
            this.consumerConfig = consumerConfig;
            this.offsetStorage = offsetStorage;
            this.topicSubcriberRegistry = topicSubcriberRegistry;
            this.loadBalanceStrategy = loadBalanceStrategy;
            this.rebalanceThread = new Thread(this);
        }


        public void start() {
            this.rebalanceThread.start();
        }


        public void stop() {
            this.stopped = true;
            this.rebalanceThread.interrupt();
            try {
                this.rebalanceThread.join(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }


        /**
         * 更新offset到zk
         */
        private void commitOffsets() {
            this.offsetStorage.commitOffset(this.consumerConfig.getGroup(), this.getTopicPartitionRegInfos());
        }


        private TopicPartitionRegInfo initTopicPartitionRegInfo(final String topic, final String group,
                                                                final Partition partition, final long offset) {
            this.offsetStorage.initOffset(topic, group, partition, offset);
            return new TopicPartitionRegInfo(topic, partition, offset);
        }


        /**
         * Returns current topic-partitions info.
         *
         * @return
         * @since 1.4.4
         */
        public Map<String/* topic */, Set<Partition>> getTopicPartitions() {
            Map<String, Set<Partition>> rt = new HashMap<String, Set<Partition>>();
            for (Map.Entry<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> entry : this.topicRegistry
                    .entrySet()) {
                rt.put(entry.getKey(), entry.getValue().keySet());
            }
            return rt;
        }


        List<TopicPartitionRegInfo> getTopicPartitionRegInfos() {
            final List<TopicPartitionRegInfo> rt = new ArrayList<TopicPartitionRegInfo>();
            for (final ConcurrentHashMap<Partition, TopicPartitionRegInfo> subMap : this.topicRegistry.values()) {
                final Collection<TopicPartitionRegInfo> values = subMap.values();
                if (values != null) {
                    rt.addAll(values);
                }
            }
            return rt;
        }


        /**
         * 加载offset信息
         *
         * @param topic
         * @param partition
         * @return
         */
        private TopicPartitionRegInfo loadTopicPartitionRegInfo(final String topic, final Partition partition) {
            return this.offsetStorage.load(topic, this.consumerConfig.getGroup(), partition);
        }

        private final BlockingQueue<Byte> rebalanceEvents = new ArrayBlockingQueue<Byte>(10);

        private final Byte REBALANCE_EVT = (byte) 1;


        public void handleChildChange(final String parentPath, final List<String> currentChilds) throws Exception {
            this.rebalanceEvents.put(this.REBALANCE_EVT);
        }


        public void run() {
            while (!this.stopped) {
                try {
                    Byte evt = this.rebalanceEvents.take();
                    if (evt != null) {
                        this.dropDuplicatedEvents();
                        this.syncedRebalance();
                    }
                } catch (InterruptedException e) {
                    // continue;
                } catch (Throwable e) {
                    log.error("Rebalance failed.", e);
                }
            }

        }


        private void dropDuplicatedEvents() {
            Byte evt = null;
            int count = 0;
            while ((evt = this.rebalanceEvents.poll()) != null) {
                // poll out duplicated events.
                count++;
            }
            if (count > 0) {
                log.info("Drop " + count + " duplicated rebalance events");
            }
        }


        boolean syncedRebalance() throws InterruptedException, Exception {
            this.rebalanceLock.lock();
            try {
                for (int i = 0; i < MAX_N_RETRIES; i++) {
                    log.info("begin rebalancing consumer " + this.consumerIdString + " try #" + i);
                    boolean done;
                    try {
                        done = this.rebalance();
                    } catch (InterruptedException e) {
                        throw e;
                    } catch (final Throwable e) {
                        // 发生了预料之外的异常,都重试一下,
                        // 有可能是多个机器consumer在同时rebalance造成的读取zk数据不一致,-- wuhua
                        log.warn("unexpected exception occured while try rebalancing", e);
                        done = false;
                    }
                    log.warn("end rebalancing consumer " + this.consumerIdString + " try #" + i);

                    if (done) {
                        log.warn("rebalance success.");
                        return true;
                    } else {
                        log.warn("rebalance failed,try #" + i);
                    }

                    // release all partitions, reset state and retry
                    this.releaseAllPartitionOwnership();
                    this.resetState();
                    // 等待zk数据同步
                    Thread.sleep(ConsumerZooKeeper.this.zkConfig.zkSyncTimeMs);
                }
                log.error("rebalance failed,finally");
                return false;
            } finally {
                this.rebalanceLock.unlock();
            }
        }


        private void resetState() {
            this.topicRegistry.clear();
            this.oldConsumersPerTopicMap.clear();
            this.oldPartitionsPerTopicMap.clear();
        }


        /**
         * 更新fetch线程
         *
         * @param cluster
         */
        protected void updateFetchRunner(final Cluster cluster) throws Exception {
            this.fetchManager.resetFetchState();
            final Set<Broker> newBrokers = new HashSet<Broker>();
            //topic报存每个topic,每个分区 消费信息
            for (final Map.Entry<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> entry : this.topicRegistry
                    .entrySet()) {
                final String topic = entry.getKey();
                for (final Map.Entry<Partition, TopicPartitionRegInfo> partEntry : entry.getValue().entrySet()) {
                    final Partition partition = partEntry.getKey();
                    final TopicPartitionRegInfo info = partEntry.getValue();
                    // 随机取master或slave的一个读,wuhua
                    final Broker broker = cluster.getBrokerRandom(partition.getBrokerId());
                    if (broker != null) {
                        newBrokers.add(broker);
                        //消费者订阅者信息
                        final SubscriberInfo subscriberInfo = this.topicSubcriberRegistry.get(topic);
                        // 添加fetch请求
                        this.fetchManager.addFetchRequest(new FetchRequest(broker, 0L, info, subscriberInfo
                                .getMaxSize()));
                    } else {
                        log.error("Could not find broker for broker id " + partition.getBrokerId()
                                + ", it should not happen.");
                    }
                }
            }

            for (Broker newOne : newBrokers) {
                int times = 0;
                Exception ne = null;
                //最多重试3次，
                while (times++ < 3) {
                    // TODO: 2019/3/9
                    try {
                        ConsumerZooKeeper.this.remotingClient.connectRomotingServer(newOne.getHost(), newOne.getPort(), 3000);
                        break;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Remote client is interrupted", e);
                    } catch (Exception e) {
                        times++;
                        ne = e;
                        continue;
                    }
                }
                if (ne != null) {
                    // Throw it to do rebalancing.
                    throw ne;
                }
            }
            // 重新启动fetch线程
            log.warn("Starting fetch runners");
            this.oldBrokerSet = newBrokers;
            this.fetchManager.startFetchRunner();
        }


        boolean rebalance() throws InterruptedException, Exception {
            //根据消费者，获取其订阅的主题 位于meta/consumers/group(具体的组名)/ids/(具体的id)
            final Map<String/* topic */, String/* consumerId */> myConsumerPerTopicMap =
                    this.getConsumerPerTopic(this.consumerIdString);
            //获取集群信息 位于meta/brokers/ids/
            final Cluster cluster = ConsumerZooKeeper.this.metaZookeeper.getCluster();
            Map<String/* topic */, List<String>/* consumer list */> consumersPerTopicMap = null;
            try {
                //获取某个分组内，每个topic到消费的目录 位于/meta/consumer/groups(具体的组名)/ids/具体的id:data消费者订阅的topic
                consumersPerTopicMap = this.getConsumersPerTopic(this.group);
            } catch (final KeeperException.NoNodeException e) {
                // 多个consumer同时在负载均衡时,可能会到达这里 -- wuhua
                log.warn("maybe other consumer is rebalancing now," + e.getMessage());
                return false;
            } catch (final ZkNoNodeException e) {
                // 多个consumer同时在负载均衡时,可能会到达这里 -- wuhua
                log.warn("maybe other consumer is rebalancing now," + e.getMessage());
                return false;
            }
            //获取每个topic的分区，meta/topic-sub/topic(具体的topic)/broker(具体的broker)：data具体的数据
            final Map<String, List<String>> partitionsPerTopicMap =
                    this.getPartitionStringsForTopics(myConsumerPerTopicMap);

            final Map<String/* topic */, String/* consumer id */> relevantTopicConsumerIdMap =
                    this.getRelevantTopicMap(myConsumerPerTopicMap, partitionsPerTopicMap,
                            this.oldPartitionsPerTopicMap, consumersPerTopicMap, this.oldConsumersPerTopicMap);
            // 没有变更，无需平衡
            if (relevantTopicConsumerIdMap.size() <= 0) {
                // 处理主备情况,topic分区和消费者没有变化,但主备的其中一台挂了,
                // 导致partitionsPerTopicMap可能是没有变化的,
                // 所以要检查集群的变化并重新连接
                if (this.checkClusterChange(cluster)) {
                    log.warn("Stopping fetch runners,maybe master or slave changed");
                    this.fetchManager.stopFetchRunner();
                    // closed all connections to old brokers.
                    this.closeOldBrokersConnections();
                    this.commitOffsets();
                    this.updateFetchRunner(cluster);
                    this.oldCluster = cluster;
                } else {
                    log.warn("Consumer " + this.consumerIdString + " with " + consumersPerTopicMap
                            + " doesn't need to be rebalanced.");
                }
                return true;
            }
            log.warn("Stopping fetch runners");
            this.fetchManager.stopFetchRunner();
            // closed all connections to old brokers.
            this.closeOldBrokersConnections();
            log.warn("Comitting all offsets");
            this.commitOffsets();

            for (final Map.Entry<String, String> entry : relevantTopicConsumerIdMap.entrySet()) {
                final String topic = entry.getKey();
                final String consumerId = entry.getValue();

                final MetaZookeeper.ZKGroupTopicDirs topicDirs =
                        ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.group);
                // 当前该topic的订阅者
                final List<String> curConsumers = consumersPerTopicMap.get(topic);
                // 当前该topic的分区
                final List<String> curPartitions = partitionsPerTopicMap.get(topic);

                if (curConsumers == null) {
                    log.warn("Releasing partition ownerships for topic:" + topic);
                    this.releasePartitionOwnership(topic);
                    this.topicRegistry.remove(topic);
                    log.warn("There are no consumers subscribe topic " + topic);
                    continue;
                }
                if (curPartitions == null) {
                    log.warn("Releasing partition ownerships for topic:" + topic);
                    this.releasePartitionOwnership(topic);
                    this.topicRegistry.remove(topic);
                    log.warn("There are no partitions under topic " + topic);
                    continue;
                }

                // 根据负载均衡策略获取这个consumer对应的partition列表
                final List<String> newParts =
                        this.loadBalanceStrategy.getPartitions(topic, consumerId, curConsumers, curPartitions);

                // 查看当前这个topic的分区列表，查看是否有变更
                ConcurrentHashMap<Partition, TopicPartitionRegInfo> partRegInfos = this.topicRegistry.get(topic);
                if (partRegInfos == null) {
                    partRegInfos = new ConcurrentHashMap<Partition, TopicPartitionRegInfo>();
                    this.topicRegistry.put(topic, new ConcurrentHashMap<Partition, TopicPartitionRegInfo>());
                }
                //原先的分区
                final Set<Partition> currentParts = partRegInfos.keySet();

                for (final Partition partition : currentParts) {
                    // 新的分区列表中不存在的分区，需要释放ownerShip，也就是老的有，新的没有
                    if (!newParts.contains(partition.toString())) {
                        log.warn("Releasing partition ownerships for partition:" + partition);
                        this.releasePartitionOwnership(topic, partition);
                        partRegInfos.remove(partition);
                    }
                }

                for (final String partition : newParts) {
                    // 当前没有的分区，挂载上去，也就是新的有，老的没有
                    if (!currentParts.contains(new Partition(partition))) {
                        log.warn(consumerId + " attempting to claim partition " + partition);
                        // 注册分区owner关系
                        if (!this.ownPartition(topicDirs, partition, topic, consumerId)) {
                            log.warn("Claim partition " + partition + " failed,retry...");
                            return false;
                        }
                    }
                }

            }
            this.updateFetchRunner(cluster);
            this.oldPartitionsPerTopicMap = partitionsPerTopicMap;
            this.oldConsumersPerTopicMap = consumersPerTopicMap;
            this.oldCluster = cluster;

            return true;
        }


        private void closeOldBrokersConnections() throws NotifyRemotingException {
            for (Broker old : this.oldBrokerSet) {
                // TODO: 2019/3/9
                ConsumerZooKeeper.this.remotingClient.colseConnectToRemoteServer(old.getHost(), old.getPort());
                log.warn("Closed " + old.getZKString());
            }
        }


        protected boolean checkClusterChange(final Cluster cluster) {
            return !this.oldCluster.equals(cluster);
        }


        protected Map<String, List<String>> getPartitionStringsForTopics(final Map<String, String> myConsumerPerTopicMap) {
            return ConsumerZooKeeper.this.metaZookeeper.getPartitionStringsForSubTopics(myConsumerPerTopicMap.keySet());
        }


        /**
         * 添加分区的owner关系,一个分区同一时刻，只能被一个组的消费者消费
         *
         * @param topicDirs
         * @param partition
         * @param topic
         * @param consumerThreadId
         * @return
         */
        //meta/consumers/group(具体的组)/ids/owners/topic(具体的topic)/partition  consumerOwnerDir
        //meta/consumers/group(具体的组)/ids/offsets/topic(具体的topic)
        protected boolean ownPartition(final MetaZookeeper.ZKGroupTopicDirs topicDirs, final String partition, final String topic,
                                       final String consumerThreadId) throws Exception {
            final String partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition;
            try {
                ZkUtils.createEphemeralPathExpectConflict(ConsumerZooKeeper.this.zkClient, partitionOwnerPath,
                        consumerThreadId); //  //meta/consumers/group(具体的组)/ids/owners/topic(具体的topic)/partition ：data消费者id
            } catch (final ZkNodeExistsException e) {
                // 原始的关系应该已经删除，所以稍候再重试
                log.info("waiting for the partition ownership to be deleted: " + partition);
                return false;

            } catch (final Exception e) {
                throw e;
            }
            this.addPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId);
            return true;
        }


        // 获取offset信息并保存到本地
        protected void addPartitionTopicInfo(final MetaZookeeper.ZKGroupTopicDirs topicDirs, final String partitionString,
                                             final String topic, final String consumerThreadId) {
            final Partition partition = new Partition(partitionString);
            final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partitionTopicInfo =
                    this.topicRegistry.get(topic);
            TopicPartitionRegInfo existsTopicPartitionRegInfo = this.loadTopicPartitionRegInfo(topic, partition);
            if (existsTopicPartitionRegInfo == null) {
                // 初始化的时候默认使用0,TODO 可能采用其他
                existsTopicPartitionRegInfo =
                        this.initTopicPartitionRegInfo(topic, consumerThreadId, partition,
                                this.consumerConfig.getOffset());// Long.MAX_VALUE
            }
            // If alwaysConsumeFromMaxOffset is set to be true,we always set
            // offset to be Long.MAX_VALUE
            if (this.consumerConfig.isAlwaysConsumeFromMaxOffset()) {
                existsTopicPartitionRegInfo.getOffset().set(Long.MAX_VALUE);
            }
            partitionTopicInfo.put(partition, existsTopicPartitionRegInfo);
        }


        /**
         * 释放分区所有权
         */
        private void releaseAllPartitionOwnership() {
            for (final Map.Entry<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> entry : this.topicRegistry
                    .entrySet()) {
                final String topic = entry.getKey();
                final MetaZookeeper.ZKGroupTopicDirs topicDirs =
                        ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.consumerConfig.getGroup());
                for (final Partition partition : entry.getValue().keySet()) {
                    final String znode = topicDirs.consumerOwnerDir + "/" + partition;
                    this.deleteOwnership(znode);
                }
            }
        }


        /**
         * 释放指定分区的ownership
         *
         * @param topic
         * @param partition
         */
        private void releasePartitionOwnership(final String topic, final Partition partition) {
            final MetaZookeeper.ZKGroupTopicDirs topicDirs =
                    ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.consumerConfig.getGroup());
            final String znode = topicDirs.consumerOwnerDir + "/" + partition;
            this.deleteOwnership(znode);
        }


        private void deleteOwnership(final String znode) {
            try {
                ZkUtils.deletePath(ConsumerZooKeeper.this.zkClient, znode);
            } catch (final Throwable t) {
                log.error("exception during releasePartitionOwnership", t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Consumer " + this.consumerIdString + " releasing " + znode);
            }
        }


        /**
         * 释放指定topic关联分区的ownership
         *
         * @param topic
         * @param
         */
        private void releasePartitionOwnership(final String topic) {
            final MetaZookeeper.ZKGroupTopicDirs topicDirs =
                    ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.consumerConfig.getGroup());
            final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partInfos = this.topicRegistry.get(topic);
            if (partInfos != null) {
                for (final Partition partition : partInfos.keySet()) {
                    final String znode = topicDirs.consumerOwnerDir + "/" + partition;
                    this.deleteOwnership(znode);
                }
            }
        }


        /**
         * 什么时候需要进行负载均衡？对于一个topic?，消费者对应的topic有变化，或者topic对应的消费者有变化
         * 当有分区增加时，需要负载均衡,
         * 当有消费者改变时需要负载均衡
         * 返回有变更的topic跟consumer集合
         *
         * @param myConsumerPerTopicMap 消费者，对应的每个topic
         * @param newPartMap            现每个topic对应的分区 /meta/topic-sub/(具体的主题)/broker
         * @param oldPartMap            原先每个topic对应的分区
         * @param newConsumerMap        现 某个分组内，某个topic对应的消费者列表
         * @param oldConsumerMap        原 某个分组内，某个topic对应的消费者列表
         * @return
         */
        private Map<String, String> getRelevantTopicMap(final Map<String, String> myConsumerPerTopicMap,
                                                        final Map<String, List<String>> newPartMap, final Map<String, List<String>> oldPartMap,
                                                        final Map<String, List<String>> newConsumerMap, final Map<String, List<String>> oldConsumerMap) {
            final Map<String, String> relevantTopicThreadIdsMap = new HashMap<String, String>();
            for (final Map.Entry<String, String> entry : myConsumerPerTopicMap.entrySet()) {
                final String topic = entry.getKey();
                final String consumerId = entry.getValue();
                // 判断分区变更或者订阅者列表是否变更
                if (!this.listEquals(oldPartMap.get(topic), newPartMap.get(topic))
                        || !this.listEquals(oldConsumerMap.get(topic), newConsumerMap.get(topic))) {
                    relevantTopicThreadIdsMap.put(topic, consumerId);
                }
            }
            return relevantTopicThreadIdsMap;
        }


        private boolean listEquals(final List<String> list1, final List<String> list2) {
            if (list1 == null && list2 != null) {
                return false;
            }
            if (list1 != null && list2 == null) {
                return false;
            }
            if (list1 == null && list2 == null) {
                return true;
            }
            return list1.equals(list2);
        }


        /**
         * 获取某个分组订阅的topic到订阅者之间的映射map
         * 获取某个分组，订阅topic到订阅者之间的映射
         *
         * @param group
         * @return
         * @throws Exception
         * @throws KeeperException.NoNodeException 多个consumer同时在负载均衡时,可能会抛出NoNodeException
         */
        protected Map<String, List<String>> getConsumersPerTopic(final String group) throws Exception, KeeperException.NoNodeException {
            final List<String> consumers =
                    ZkUtils.getChildren(ConsumerZooKeeper.this.zkClient, this.dirs.consumerRegistryDir);
            if (consumers == null) {
                return Collections.emptyMap();
            }
            final Map<String, List<String>> consumersPerTopicMap = new HashMap<String, List<String>>();
            for (final String consumer : consumers) {
                final List<String> topics = this.getTopics(consumer);// 多个consumer同时在负载均衡时,这里可能会抛出NoNodeException，--wuhua
                for (final String topic : topics) {
                    if (consumersPerTopicMap.get(topic) == null) {
                        final List<String> list = new ArrayList<String>();
                        list.add(consumer);
                        consumersPerTopicMap.put(topic, list);
                    } else {
                        consumersPerTopicMap.get(topic).add(consumer);
                    }
                }

            }
            // 订阅者排序
            for (final Map.Entry<String, List<String>> entry : consumersPerTopicMap.entrySet()) {
                Collections.sort(entry.getValue());
            }
            return consumersPerTopicMap;
        }


        public Map<String, String> getConsumerPerTopic(final String consumerId) throws Exception {
            final List<String> topics = this.getTopics(consumerId);
            final Map<String/* topic */, String/* consumerId */> rt = new HashMap<String, String>();
            for (final String topic : topics) {
                rt.put(topic, consumerId);
            }
            return rt;
        }


        /**
         * 根据consumerId获取订阅的topic列表
         *
         * @param consumerId
         * @return
         * @throws Exception
         */
        protected List<String> getTopics(final String consumerId) throws Exception {
            final String topicsString =
                    ZkUtils.readData(ConsumerZooKeeper.this.zkClient, this.dirs.consumerRegistryDir + "/" + consumerId);
            if (StringUtils.isBlank(topicsString)) {
                return Collections.emptyList();
            }
            final String[] topics = topicsString.split(",");
            final List<String> rt = new ArrayList<String>(topics.length);
            for (final String topic : topics) {
                rt.add(topic);
            }
            return rt;
        }
    }
}
