package com.lin.client.consumer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lin.client.MetaMessageSessionFactory;
import com.lin.client.ResponseFuture;
import com.lin.client.consumer.storage.OffsetStorage;
import com.lin.client.consumer.strategy.LoadBalanceStrategy;
import com.lin.client.netty.RemoteClient;
import com.lin.client.producer.ProducerZooKeeper;
import com.lin.commons.Message;
import com.lin.commons.MessageIterator;
import com.lin.commons.cluster.Broker;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.exception.SimpleMQOpeartionTimeoutException;
import com.lin.commons.filter.ConsumerMessageFilter;
import com.lin.commons.utils.ConcurrentHashSet;
import com.lin.commons.utils.JSONUtils;
import com.lin.commons.utils.LongSequenceGenerator;
import com.lin.commons.utils.ZkUtils;
import com.lin.commons.utils.network.Header;
import com.lin.commons.utils.network.MessageTypeContant;
import com.lin.commons.utils.network.Request;
import com.lin.commons.utils.network.Response;
import com.lin.commons.utils.zk.MetaZookeeper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午5:26
 */
public class SimpleMessageConsumer implements MessageConsumer, FetchMessage {

    public static Logger logger = LoggerFactory.getLogger(SimpleMessageConsumer.class);

    private static final int DEFAULT_OP_TIMEOUT = 100000;

//    static final Log log = LogFactory.egetLog(FetchRequestRunner.class);

    private final RemoteClient remoteClient; //远程交流客户端

    private final ConsumerConfig consumerConfig; //消费者配置

    private final ConsumerZooKeeper consumerZooKeeper; //消费者与zookeeper交互

    private final MetaMessageSessionFactory messageSessionFactory;

    private final OffsetStorage offsetStorage;

    private final LoadBalanceStrategy loadBalanceStrategy; //负载均衡策略

    private final ProducerZooKeeper producerZooKeeper;

    private final ScheduledExecutorService scheduledExecutorService; //定时调度器，采用push模式，定时拉取

    private final SubscribeInfoManager subscribeInfoManager; //订阅信息管理器

    private final RecoverManager recoverStorageManager; //消费者的recover管理器

    //
    private final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry =
            new ConcurrentHashMap<String, SubscriberInfo>();

    private FetchManager fetchManager;

    private final ConcurrentHashSet<String> publishedTopics = new ConcurrentHashSet<String>();

//    private RejectConsumptionHandler rejectConsumptionHandler;


    public SimpleMessageConsumer(final MetaMessageSessionFactory messageSessionFactory,
                                 final RemoteClient remotingClient, final ConsumerConfig consumerConfig,
                                 final ConsumerZooKeeper consumerZooKeeper, final ProducerZooKeeper producerZooKeeper,
                                 final SubscribeInfoManager subscribeInfoManager, final RecoverManager recoverManager,
                                 final OffsetStorage offsetStorage, final LoadBalanceStrategy loadBalanceStrategy) {
        super();
        this.messageSessionFactory = messageSessionFactory;
        this.remoteClient = remotingClient;
        this.consumerConfig = consumerConfig;
        this.producerZooKeeper = producerZooKeeper;
        this.consumerZooKeeper = consumerZooKeeper;
        this.offsetStorage = offsetStorage;
        this.subscribeInfoManager = subscribeInfoManager;
        this.recoverStorageManager = recoverManager;
        // TODO: 2019/3/15
//        this.fetchManager =   new SimpleFetchManager(consumerConfig, this);
        this.fetchManager = new SimpleFetchManager(consumerConfig, this);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();//单个线程运行
        this.loadBalanceStrategy = loadBalanceStrategy;
        // Use local recover policy by default.
//        this.rejectConsumptionHandler = new LocalRecoverPolicy(this.recoverStorageManager);
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() { //定时提交offset到zk中
                                                              @Override
                                                              public void run() {
                                                                  SimpleMessageConsumer.this.consumerZooKeeper.commitOffsets(SimpleMessageConsumer.this.fetchManager);
                                                              }
                                                          }, consumerConfig.getCommitOffsetPeriodInMills(), consumerConfig.getCommitOffsetPeriodInMills(),
                TimeUnit.MILLISECONDS);
    }


    public MetaMessageSessionFactory getParent() {
        return this.messageSessionFactory;
    }


    public FetchManager getFetchManager() {
        return this.fetchManager;
    }


    void setFetchManager(final FetchManager fetchManager) {
        this.fetchManager = fetchManager;
    }


    ConcurrentHashMap<String, SubscriberInfo> getTopicSubcriberRegistry() {
        return this.topicSubcriberRegistry;
    }


    @Override
    public OffsetStorage getOffsetStorage() {
        return this.offsetStorage;
    }


    @Override
    public synchronized void shutdown() throws SimpleMQClientException {
        if (this.fetchManager.isShutdown()) {
            return;
        }
        try {
            this.fetchManager.stopFetchRunner();
            this.consumerZooKeeper.unRegisterConsumer(this.fetchManager);
            for (String topic : this.publishedTopics) {
                this.producerZooKeeper.unPublishTopic(topic, this);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            this.scheduledExecutorService.shutdownNow();
            this.offsetStorage.close();
            // 删除本组的订阅关系
            this.subscribeInfoManager.removeGroup(this.consumerConfig.getGroup());
            this.messageSessionFactory.removeChild(this);
        }

    }


    @Override
    public MessageConsumer subscribe(final String topic, final int maxSize, final MessageListener messageListener)
            throws SimpleMQClientException {
        return this.subscribe(topic, maxSize, messageListener, null);
    }


    /**
     * 确保一个进程内，一个topic只能被一个组的consumer订阅
     *
     * @param topic           订阅的topic
     * @param maxSize         订阅每次接收的最大数据大小
     * @param messageListener
     * @param filter
     * @return
     * @throws SimpleMQClientException
     */
    @Override
    public MessageConsumer subscribe(final String topic, final int maxSize, final MessageListener messageListener,
                                     ConsumerMessageFilter filter) throws SimpleMQClientException {
        this.checkState();
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Blank topic");
        }
        if (messageListener == null) {
            throw new IllegalArgumentException("Null messageListener");
        }
        // 先添加到公共管理器
        this.subscribeInfoManager.subscribe(topic, this.consumerConfig.getGroup(), maxSize, messageListener, filter);
        // 然后添加到自身的管理器
        SubscriberInfo info = this.topicSubcriberRegistry.get(topic);
        if (info == null) {
            info = new SubscriberInfo(messageListener, filter, maxSize);
            final SubscriberInfo oldInfo = this.topicSubcriberRegistry.putIfAbsent(topic, info);
            if (oldInfo != null) {
                throw new SimpleMQClientException("Topic=" + topic + " has been subscribered");
            }
            return this;
        } else {
            throw new SimpleMQClientException("Topic=" + topic + " has been subscribered");
        }
    }


    //    @Override
    public void appendCouldNotProcessMessage(final Message message) throws IOException {
//        if (log.isInfoEnabled()) {
//            log.info("Message could not process,save to local.MessageId=" + message.getId() + ",Topic="
//                    + message.getTopic() + ",Partition=" + message.getPartition());
//        }
//        if (this.rejectConsumptionHandler != null) {
//            this.rejectConsumptionHandler.rejectConsumption(message, this);
//        }
    }


    private void checkState() {
        if (this.fetchManager.isShutdown()) {
            throw new IllegalStateException("Consumer has been shutdown");
        }
    }


    @Override
    public void completeSubscribe() throws SimpleMQClientException {
        this.checkState();
        try {
            this.consumerZooKeeper.registerConsumer(this.consumerConfig, this.fetchManager,
                    this.topicSubcriberRegistry, this.offsetStorage, this.loadBalanceStrategy);
        } catch (final Exception e) {
            throw new SimpleMQClientException("注册订阅者失败", e);
        }
    }


    //    @Override
    public MessageListener getMessageListener(final String topic) {
        final SubscriberInfo info = this.topicSubcriberRegistry.get(topic);
        if (info == null) {
            return null;
        }
        return info.getMessageListener();
    }


    //    @Override
    public ConsumerMessageFilter getMessageFilter(final String topic) {
        final SubscriberInfo info = this.topicSubcriberRegistry.get(topic);
        if (info == null) {
            return null;
        }
        return info.getConsumerMessageFilter();
    }


    //    查询当前的offset @Override
    public long offset(final FetchRequest fetchRequest) throws SimpleMQClientException {
//        final long start = System.currentTimeMillis();
//        boolean success = false;
        String topic = fetchRequest.getTopic();
        String group = this.consumerConfig.getGroup();
        Partition partition = fetchRequest.getPartitionObject();
//        MetaZookeeper metaZookeeper = this.consumerZooKeeper.metaZookeeper;
        final MetaZookeeper.ZKGroupTopicDirs topicDirs = this.consumerZooKeeper.metaZookeeper.new ZKGroupTopicDirs(topic, group);
        //获取消费者在当前分区的进度
        String completePath = topicDirs.consumerOffsetDir + "/" + partition.toString();
        try {
            ZkUtils.makeSurePersistentPathExists(consumerZooKeeper.zkClient, completePath);
            String offsetString = ZkUtils.readData(consumerZooKeeper.zkClient, completePath);
            String[] offsets = offsetString.split("-");
            return Long.parseLong(offsets[1]);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("fail to get offset because of :{}", e);
            throw new SimpleMQClientException(e);
        }
    }

    //fetch抓取消息用
//    @Override
    public MessageIterator fetch(final FetchRequest fetchRequest, long timeout, TimeUnit timeUnit)
            throws SimpleMQClientException, InterruptedException {
        if (timeout <= 0 || timeUnit == null) {
            timeout = this.consumerConfig.getFetchTimeoutInMills();
            timeUnit = TimeUnit.MILLISECONDS;
        }

        if (timeout < 0 || timeUnit == null) { //没有则从配置中获取
            timeout = this.getConsumerConfig().getFetchTimeoutInMills();
            timeUnit = TimeUnit.MICROSECONDS;
        }
        final long start = System.currentTimeMillis();
        boolean success = false; //表示是否抓取信息成功
        try {
            String host = fetchRequest.getBroker().getHost();
            int port = fetchRequest.getBroker().getPort();
//            final long currentOffset = fetchRequest.getOffset();//获取当前的消费者消费的偏移量
            if (!remoteClient.isConnected(host, port)) {
                ConsumerZooKeeper.ZKLoadRebalanceListener listener =
                        this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager);
                if (listener.oldBrokerSet.contains(fetchRequest.getBroker())) {
                    this.remoteClient.connectRomotingServer(host, port, 60 * 1000);
                }
                return null;
            }
            Request request = generateGetMessageRequest(fetchRequest, this.getConsumerConfig().getGroup());
            Partition partition = fetchRequest.getPartitionObject();// TODO: 2019/3/16
            ResponseFuture<Response, Request> responseFuture = remoteClient.sendMessageToServer(host, port, request, partition);
            Response response = responseFuture.get(DEFAULT_OP_TIMEOUT, TimeUnit.SECONDS);
            //解析响应返回messageIterator
            return analysisResponse(response);
        } catch (TimeoutException e) {
            logger.error("fail to fetch from host:{},port:{} because of :{} ", fetchRequest.getBroker().getHost(), fetchRequest.getBroker().getPort(), e);
            throw new SimpleMQOpeartionTimeoutException(e);
        } catch (ExecutionException e) {
            logger.error("fail to fetch from host:{},port:{} because of:{} ", fetchRequest.getBroker().getHost(), fetchRequest.getBroker().getPort(), e);
            throw new SimpleMQClientException(e);
        } catch (Exception e) {
            logger.error("fail to fetch from host:{},port:{} because of:{} ", fetchRequest.getBroker().getHost(), fetchRequest.getBroker().getPort(), e);
            throw new SimpleMQClientException(e);
        }

    }


//    @Override
//    public void setSubscriptions(final Collection<Subscription> subscriptions) throws MetaClientException {
//        if (subscriptions == null) {
//            return;
//        }
//        for (final Subscription subscription : subscriptions) {
//            this.subscribe(subscription.getTopic(), subscription.getMaxSize(), subscription.getMessageListener());
//        }
//    }

    /**
     * 解析响应返回MessageIterator
     *
     * @return
     */
    private MessageIterator analysisResponse(Response response) {
        String responseString = response.getBody();
        JSONObject responsejson = JSONUtils.getJSONObject(responseString);
        String error = responsejson.getString("error");
        if (!org.apache.commons.lang3.StringUtils.isBlank(error)) { //如果获取失败
            logger.error("fail to  get message because of :{}", error);
            return null;
        }
        JSONArray jsonArray = responsejson.getJSONArray("message");
        String topic = responsejson.getString("topic");
        return new MessageIterator(topic, jsonArray);

    }


    private Request generateGetMessageRequest(FetchRequest fetchRequest, String group) {

        Request request = new Request();
        Header header = new Header();
        header.setReq((byte) 0);
        header.setType(MessageTypeContant.GET_MESSAGE);
        header.setSessionID(-1);
        header.setRequestId(LongSequenceGenerator.getNextSequenceId());
        request.setHeader(header);
        JSONObject requestJson = new JSONObject();
        requestJson.put("currentOffset", fetchRequest.getOffset());
        requestJson.put("size", fetchRequest.getMaxSize());
        requestJson.put("group", group);
        requestJson.put("partition", fetchRequest.getPartition());
        requestJson.put("topic", fetchRequest.getTopic());
        requestJson.put("brokerId", fetchRequest.getBrokerId());
        request.setBody(JSONUtils.toJsonString(requestJson));
        return request;
    }


    @Override
    public MessageIterator get(final String topic, final Partition partition, final long offset, final int maxSize,
                               final long timeout, final TimeUnit timeUnit) throws SimpleMQClientException, InterruptedException {
        if (!this.publishedTopics.contains(topic)) {
            this.producerZooKeeper.publishTopic(topic, this);
            this.publishedTopics.add(topic);
        }
        final Broker broker =
                new Broker(partition.getBrokerId(), this.producerZooKeeper.selectBroker(topic, partition));
        final TopicPartitionRegInfo topicPartitionRegInfo = new TopicPartitionRegInfo(topic, partition, offset);
        return this.fetch(new FetchRequest(broker, 0, topicPartitionRegInfo, maxSize), timeout, timeUnit);
    }


//    @Override
//    public RejectConsumptionHandler getRejectConsumptionHandler() {
//        return this.rejectConsumptionHandler;
//    }


//    @Override
//    public void setRejectConsumptionHandler(RejectConsumptionHandler rejectConsumptionHandler) {
//        if (rejectConsumptionHandler == null) {
//            throw new NullPointerException("Null rejectConsumptionHandler");
//        }
//        this.rejectConsumptionHandler = rejectConsumptionHandler;
//    }


    @Override
    public ConsumerConfig getConsumerConfig() {
        return this.consumerConfig;
    }


    @Override
    public MessageIterator get(final String topic, final Partition partition, final long offset, final int maxSize)
            throws SimpleMQClientException, InterruptedException {
        return this.get(topic, partition, offset, maxSize, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * Created with IntelliJ IDEA. User: dennis (xzhuang@avos.com) Date: 13-2-5
     * Time: 上午11:29
     */
//    public static class DropPolicy implements RejectConsumptionHandler {
//        @Override
//        public void rejectConsumption(Message message, MessageConsumer messageConsumer) {
//            // Drop the message.
//        }
//    }
//
//    /**
//     * Created with IntelliJ IDEA. User: dennis (xzhuang@avos.com) Date: 13-2-5
//     * Time: 上午11:25
//     */
//    public static class LocalRecoverPolicy implements RejectConsumptionHandler {
//        private final RecoverManager recoverManager;
//        static final Log log = LogFactory.getLog(LocalRecoverPolicy.class);
//
//
//        public LocalRecoverPolicy(RecoverManager recoverManager) {
//            this.recoverManager = recoverManager;
//        }
//
//
//        @Override
//        public void rejectConsumption(Message message, MessageConsumer messageConsumer) {
//            try {
//                this.recoverManager.append(messageConsumer.getConsumerConfig().getGroup(), message);
//            } catch (IOException e) {
//                log.error("Append message to local recover manager failed", e);
//            }
//        }
//    }

}
