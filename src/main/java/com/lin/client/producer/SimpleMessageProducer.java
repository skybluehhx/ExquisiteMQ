package com.lin.client.producer;

import com.alibaba.fastjson.JSONObject;
import com.lin.client.MessageUtils;
import com.lin.client.MetaMessageSessionFactory;
import com.lin.client.ResponseFuture;
import com.lin.client.netty.RemoteClient;
import com.lin.client.netty.SingleRequestCallBackListener;
import com.lin.commons.Message;
import com.lin.commons.MessageAccessor;
import com.lin.commons.PartitionSelector;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.InvalidMessageException;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.exception.SimpleMQOpeartionTimeoutException;
import com.lin.commons.utils.ConcurrentHashSet;
import com.lin.commons.utils.JSONUtils;
import com.lin.commons.utils.network.MessageTypeContant;
import com.lin.commons.utils.network.Request;
import com.lin.commons.utils.network.Response;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 消费生产者实现
 *
 * @author jianglinzou
 * @date 2019/3/11 下午1:21
 */
public class SimpleMessageProducer implements MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleMessageProducer.class);
    protected static final int DEFAULT_OP_TIMEOUT = 3000;
    private static final int TIMEOUT_THRESHOLD = Integer.parseInt(System.getProperty("meta.send.timeout.threshold",
            "200"));
    private final MetaMessageSessionFactory messageSessionFactory;
    protected final RemoteClient remotingClient;
    protected final PartitionSelector partitionSelector;
    protected final ProducerZooKeeper producerZooKeeper;
    protected final long sessionId;
    private volatile boolean shutdown;
    private static final int MAX_RETRY = 1;
    private final ConcurrentHashSet<String> publishedTopics = new ConcurrentHashSet<String>();//存放当前已推送topic主题集合
    protected long transactionRequestTimeoutInMills = 5000;


    public SimpleMessageProducer(final MetaMessageSessionFactory messageSessionFactory,
                                 final RemoteClient remotingClient, final PartitionSelector partitionSelector,
                                 final ProducerZooKeeper producerZooKeeper, final long sessionId) {
        super();
        this.sessionId = sessionId;
        this.messageSessionFactory = messageSessionFactory;
        this.remotingClient = remotingClient;
        this.partitionSelector = partitionSelector;
        this.producerZooKeeper = producerZooKeeper;
        // this.ordered = ordered;
    }

    public MetaMessageSessionFactory getParent() {
        return this.messageSessionFactory;
    }


    public PartitionSelector getPartitionSelector() {
        return this.partitionSelector;
    }


    @Deprecated
    public boolean isOrdered() {
        return false;
    }


    public void publish(final String topic) {
        this.checkState();
        this.checkTopic(topic);
        // It is not always synchronized with shutdown,but it is acceptable.
        if (!this.publishedTopics.contains(topic)) {
            this.producerZooKeeper.publishTopic(topic, this); //发表主题
            this.publishedTopics.add(topic);
        }
        // this.localMessageStorageManager.setMessageRecoverer(this.recoverer);
    }


    public void setDefaultTopic(final String topic) {
        // It is not always synchronized with shutdown,but it is acceptable.
        if (!this.publishedTopics.contains(topic)) {
            this.producerZooKeeper.setDefaultTopic(topic, this);
            this.publishedTopics.add(topic);
        }
    }


    private void checkTopic(final String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Blank topic:" + topic);
        }
    }

    static final Pattern RESULT_SPLITER = Pattern.compile(" ");


    public SendResult sendMessage(final Message message, final int timeout, final TimeUnit unit)
            throws SimpleMQClientException, InterruptedException {
        this.checkState();
        this.checkMessage(message);
        return this.sendMessageToServer(message, timeout, unit);
    }


    /**
     * 正常的消息发送到服务器
     */
    protected SendResult sendMessageToServer(final Message message, final int timeout, final TimeUnit unit)
            throws SimpleMQClientException, InterruptedException, SimpleMQOpeartionTimeoutException {
        /**
         * 异常信息处理的原则：
         * <ul>
         * <li>客户端错误信息都以异常的形式抛出，全部转化为MetaClientException的check异常</li>
         * <li>服务器返回失败的异常信息，作为SendResult的结果返回给客户端，不作为异常抛出</li>
         * </ul>
         */
        this.checkState();
        this.checkMessage(message);
        SendResult result = null;
        final long start = System.currentTimeMillis();
        int retry = 0;
        final long timeoutInMills = TimeUnit.MILLISECONDS.convert(timeout, unit);
        try {
            for (int i = 0; i < MAX_RETRY; i++) {
                result = this.send0(message, timeout, unit);
                if (result.isSuccess()) {
                    break;
                }
                if (System.currentTimeMillis() - start >= timeoutInMills) {
                    throw new SimpleMQOpeartionTimeoutException("Send message timeout in " + timeoutInMills + " mills");
                }
                retry++;
            }
        } finally {
            final long duration = System.currentTimeMillis() - start;
            if (duration > TIMEOUT_THRESHOLD) {
            }
            if (retry > 0) {
            }
        }
        return result;
    }


    private Partition selectPartition(final Message message) throws SimpleMQClientException {
        return this.producerZooKeeper.selectPartition(message.getTopic(), message, this.partitionSelector);
    }

    public long getSessionId() {
        return this.sessionId;
    }

    private SendResult send0(final Message message, final int timeout, final TimeUnit unit)
            throws InterruptedException, SimpleMQClientException {
        try {
            final String topic = message.getTopic();
            Partition partition = null;
            String serverUrl = null;
            if (partition == null) {
                partition = this.selectPartition(message);
                MessageAccessor.setPartition(message, partition);

            }
            if (partition == null) {
                throw new SimpleMQClientException("There is no aviable partition for topic " + topic
                        + ",maybe you don't publish it at first?");
            }
            if (serverUrl == null) {
                serverUrl = this.producerZooKeeper.selectBroker(topic, partition);
            }
            if (serverUrl == null) {
                throw new SimpleMQClientException("There is no aviable server right now for topic " + topic
                        + " and partition " + partition + ",maybe you don't publish it at first?");
            }
            Request request = MessageUtils.generateRequest(message, sessionId);
            final Response resp = this.invokeToGroup(serverUrl, partition, request, timeout, unit);
            return this.genSendResult(message, partition, serverUrl, resp);
        } catch (final InterruptedException e) {
            throw e;
        } catch (final SimpleMQClientException e) {
            throw e;
        } catch (final ExecutionException e) {
            throw new SimpleMQOpeartionTimeoutException("Send message timeout in "
                    + TimeUnit.MILLISECONDS.convert(timeout, unit) + " mills because of" + e.getMessage());
        } catch (final Exception e) {
            throw new SimpleMQClientException("send message failed because" + e);
        }

    }


    private SendResult genSendResult(final Message message, final Partition partition, final String serverUrl, Response resp) {
        // todo
        if (MessageTypeContant.RESP_SEND_MESSAGE == resp.getHeader().getType()) {
            String body = resp.getBody();
            JSONObject jsonObject = JSONUtils.getJSONObject(body);
            Long messageId = jsonObject.getLong("messageId");
            if (Objects.isNull(messageId)) {
                return new SendResult(false, null, -1l, "服务器内部出错");
            }
            MessageAccessor.setId(message, messageId);
            if (messageId < 0L) {
                return new SendResult(false, null, -1l, jsonObject.getString("error"));
            }
            return new SendResult(true, message.getPartition(), messageId, null);
        }

        return null;
    }


    protected Response invokeToGroup(final String serverUrl, final Partition partition,
                                     final Request request, final int timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, URISyntaxException, SimpleMQClientException {
        URI uri = new URI(serverUrl);

        log.info("359 SimpleMessageProducer+ will send message to server the host:" + uri.getHost() + "the port" + uri.getPort());
        ResponseFuture<Response, Request> responseFuture = this.remotingClient.sendMessageToServer(uri.getHost(), uri.getPort(), request, partition);
        Response response;
        if (timeout <= 0) {
            response = responseFuture.get();
        } else {
            response = responseFuture.get(timeout, unit);
        }
        return response;
    }


    protected void checkState() {
        if (this.shutdown) {
            throw new IllegalStateException("Producer has been shutdown");
        }
    }


    protected void checkMessage(final Message message) throws SimpleMQClientException {
        if (message == null) {
            throw new InvalidMessageException("Null message");
        }
        if (StringUtils.isBlank(message.getTopic())) {
            throw new InvalidMessageException("Blank topic");
        }
        if (message.getData() == null) {
            throw new InvalidMessageException("Null data");
        }
    }


    public void sendMessage(final Message message, final SendMessageCallback cb, final int time, final TimeUnit unit) {
        try {
            this.checkState();
            this.checkMessage(message);
            final String topic = message.getTopic();
            final Partition partition = this.selectPartition(message);
            if (partition == null) {
                throw new SimpleMQClientException("There is no aviable partition for topic " + topic
                        + ",maybe you don't publish it at first?");
            }
            MessageAccessor.setPartition(message, partition);
            final String serverUrl = this.producerZooKeeper.selectBroker(topic, partition);
            if (serverUrl == null) {
                throw new SimpleMQClientException("There is no aviable server right now for topic " + topic
                        + " and partition " + partition + ",maybe you don't publish it at first?");
            }
            Request request = MessageUtils.generateRequest(message, sessionId);
            URI uri = new URI(serverUrl);
            this.remotingClient.sendMessageToServer(uri.getHost(), uri.getPort(), request, partition, new SingleRequestCallBackListener() {
                @Override
                public void onResponse(Response response) {
                    final SendResult rt = SimpleMessageProducer.this.genSendResult(message, partition, serverUrl, response);
                    cb.onMessageSent(rt);
                }

                @Override
                public void onException(Exception exception) {
                    cb.onException(exception);
                }

                @Override
                public Executor getExecutor() {
                    return null;
                }
            }, time, unit);


        } catch (final Throwable e) {
            cb.onException(e);
        }
        return;
    }


    public void sendMessage(final Message message, final SendMessageCallback cb) {
        this.sendMessage(message, cb, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
    }


    public SendResult sendMessage(final Message message) throws SimpleMQClientException, InterruptedException {
        return this.sendMessage(message, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
    }


    public synchronized void shutdown() throws SimpleMQClientException {
        if (this.shutdown) {
            return;
        }
        for (String topic : this.publishedTopics) {
            this.producerZooKeeper.unPublishTopic(topic, this);
        }
        this.shutdown = true;
        this.publishedTopics.clear();
        this.messageSessionFactory.removeChild(this);
    }
}
