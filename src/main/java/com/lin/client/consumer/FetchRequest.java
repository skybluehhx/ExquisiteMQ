package com.lin.client.consumer;

import com.lin.commons.cluster.Broker;
import com.lin.commons.cluster.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 一次获取请求
 *
 * @author jianglinzou
 * @date 2019/3/11 下午1:24
 */
public class FetchRequest implements Delayed {

    public static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;
    // 延后的时间戳
    private long delayTimeStamp;
    private long delay;
    private TopicPartitionRegInfo topicPartitionRegInfo; //topic的订阅信息，有起始的offset
    private int maxSize;
    private int originalMaxSize;
    private Broker broker;
    private int retries = 0;
    private long tmpOffset;
    private FetchRequestQueue refQueue;


    public TopicPartitionRegInfo getTopicPartitionRegInfo() {
        return this.topicPartitionRegInfo;
    }


    public FetchRequestQueue getRefQueue() {
        return this.refQueue;
    }


    public void setRefQueue(FetchRequestQueue refQueue) {
        this.refQueue = refQueue;
    }


    /**
     * Just for test
     *
     * @param delay
     */
    FetchRequest(final long delay) {
        super();
        this.delay = delay;
        this.delayTimeStamp = System.currentTimeMillis() + delay;
    }


    public int getRetries() {
        return this.retries;
    }

    static final Log log = LogFactory.getLog(FetchRequest.class);


    public void increaseMaxSize() {
//        if (this.maxSize > MessageUtils.MAX_READ_BUFFER_SIZE) {
//            log.warn("警告：maxSize超过最大限制" + MessageUtils.MAX_READ_BUFFER_SIZE
//                + "Bytes，请设置环境变量-Dnotify.remoting.max_read_buffer_size超过此限制");
//            return;
//        }
        if (maxSize < 0) {
            log.warn("警告，maxSize已溢出");
            maxSize = 1;
        }
        this.maxSize = this.maxSize + this.maxSize / 2;
    }


    public void decreaseMaxSize() {
        if (this.maxSize < this.originalMaxSize) {
            this.maxSize = this.originalMaxSize;
        } else if (this.maxSize == this.originalMaxSize) {
            return;
        } else {
            this.maxSize = this.maxSize / 2;
        }
    }


    public void resetRetries() {
        this.retries = 0;
    }


    public int incrementRetriesAndGet() {
        return ++this.retries;
    }


    public FetchRequest(final Broker broker, final long delay, final TopicPartitionRegInfo topicPartitionRegInfo,
                        final int maxSize) {
        super();
        this.broker = broker;
        this.delay = delay;
        if (delay >= 0) {
            this.delayTimeStamp = System.currentTimeMillis() + delay;
        }
        this.topicPartitionRegInfo = topicPartitionRegInfo;
        this.maxSize = maxSize;
        this.originalMaxSize = maxSize;
        if (this.maxSize <= 0) {
            throw new IllegalArgumentException("maxSize <=0");
        }
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.broker == null ? 0 : this.broker.hashCode());
        result = prime * result + (int) (this.delay ^ this.delay >>> 32);
        result = prime * result + this.maxSize;
        result = prime * result + this.retries;
        result = prime * result + (this.topicPartitionRegInfo == null ? 0 : this.topicPartitionRegInfo.hashCode());
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final FetchRequest other = (FetchRequest) obj;
        if (this.broker == null) {
            if (other.broker != null) {
                return false;
            }
        } else if (!this.broker.equals(other.broker)) {
            return false;
        }
        if (this.delay != other.delay) {
            return false;
        }
        if (this.maxSize != other.maxSize) {
            return false;
        }
        if (this.retries != other.retries) {
            return false;
        }
        if (this.topicPartitionRegInfo == null) {
            if (other.topicPartitionRegInfo != null) {
                return false;
            }
        } else if (!this.topicPartitionRegInfo.equals(other.topicPartitionRegInfo)) {
            return false;
        }
        return true;
    }


    public Broker getBroker() {
        return this.broker;
    }


    public void setBroker(final Broker broker) {
        this.broker = broker;
    }


    /**
     * 设置延后的时间，单位毫秒
     *
     * @param delay
     */
    public void setDelay(final long delay) {
        this.delay = delay;
        this.delayTimeStamp = System.currentTimeMillis() + delay;
    }


    public int compareTo(final Delayed o) {
        if (o == this) {
            return 0;
        }
        final FetchRequest other = (FetchRequest) o;
        final long sub = this.delayTimeStamp - other.delayTimeStamp;
        if (sub == 0) {
            return 0;
        } else {
            return sub < 0 ? -1 : 1;
        }
    }


    public long getDelay() {
        return this.delay;
    }


    public long getDelay(final TimeUnit unit) {
        return unit.convert(this.delayTimeStamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }


    public int getMaxSize() {
        return this.maxSize;
    }


    /**
     * 更新offset，当ack为true则更新存储中的offset，并将临时offset设置为－1,否则仅更新临时offset
     *
     * @param offset
     * @param ack
     */
    public void setOffset(final long offset, final long msgId, final boolean ack) {
        if (ack) {
            //应为会异步，定时对topicPartitionRegInfo的信息定时提交到zk上，所以需要
            //加锁，确保对topicPartitionRegInfo的修改的原子性
            // 对topicPartitionRegInfo加锁，防止提交到zk不一致
            synchronized (this.topicPartitionRegInfo) {
                this.topicPartitionRegInfo.getOffset().set(offset);
                if (msgId != -1) {
                    this.topicPartitionRegInfo.setMessageId(msgId);
                }
                // 有变更，需要更新到storage
                this.topicPartitionRegInfo.setModified(true);
            }
            this.rollbackOffset();
        } else {
            this.tmpOffset = offset;
        }
    }


    public String getTopic() {
        return this.topicPartitionRegInfo.getTopic();
    }


    public int getPartition() {
        return this.topicPartitionRegInfo.getPartition().getPartition();
    }

    public int getBrokerId() {
        return this.topicPartitionRegInfo.getPartition().getBrokerId();
    }


    public Partition getPartitionObject() {
        return this.topicPartitionRegInfo.getPartition();

    }


    public void rollbackOffset() {
        this.tmpOffset = -1;
    }


    public long getLastMessageId() {
        return this.topicPartitionRegInfo.getMessageId();
    }


    /**
     * 返回将要使用的offset，如果有临时offset，则优先使用临时offset
     *
     * @return
     */
    public long getOffset() {
        if (this.tmpOffset > 0) {
            return this.tmpOffset;
        } else {
            return this.topicPartitionRegInfo.getOffset().get();
        }
    }

}
