package com.lin.client.consumer;

import com.lin.client.Shutdownable;
import com.lin.client.consumer.storage.OffsetStorage;
import com.lin.commons.MessageIterator;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.filter.ConsumerMessageFilter;

import java.util.concurrent.TimeUnit;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午4:59
 */
public interface MessageConsumer extends Shutdownable {


    /**
     * 获取指定topic和分区下面的消息，默认超时10秒
     *
     * @param topic
     * @param partition
     * @return 消息迭代器，可能为null
     */
    public MessageIterator get(String topic, Partition partition, long offset, int maxSize) throws SimpleMQClientException,
            InterruptedException;


    /**
     * 获取指定topic和分区下面的消息，在指定时间内没有返回则抛出异常
     *
     * @param topic
     * @param partition
     * @param timeout
     * @param timeUnit
     * @return 消息迭代器，可能为null
     * @throws TimeoutException
     */
    public MessageIterator get(String topic, Partition partition, long offset, int maxSize, long timeout,
                               TimeUnit timeUnit) throws SimpleMQClientException, InterruptedException;


    /**
     * 订阅指定的消息，传入MessageListener，当有消息达到的时候主动通知MessageListener，请注意，
     * 调用此方法并不会使订阅关系立即生效， 只有在调用complete方法后才生效，此方法可做链式调用
     *
     * @param topic
     *            订阅的topic
     * @param maxSize
     *            订阅每次接收的最大数据大小
     * @param messageListener
     *            消息监听器
     */
    public MessageConsumer subscribe(String topic, int maxSize, MessageListener messageListener)
            throws SimpleMQClientException;


    /**
     * 订阅指定的消息，传入MessageListener和ConsumerMessageFilter，
     * 当有消息到达并且ConsumerMessageFilter
     * #accept返回true的时候,主动通知MessageListener该条消息，请注意， 调用此方法并不会使订阅关系立即生效，
     * 只有在调用complete方法后才生效，此方法可做链式调用
     *
     * @param topic
     *            订阅的topic
     * @param maxSize
     *            订阅每次接收的最大数据大小
     * @param messageListener
     * @param ConsumerMessageFilter
     *            message filter 消息监听器
     */
    public MessageConsumer subscribe(String topic, int maxSize, MessageListener messageListener,
                                     ConsumerMessageFilter consumerMessageFilter) throws SimpleMQClientException;


    /**
     * 批量订阅消息,请注意，调用此方法并不会使订阅关系立即生效，只有在调用complete方法后才生效。
     *
     * @param subscriptions
     */
//    public void setSubscriptions(Collection<Subscription> subscriptions) throws SimpleMQClientException;


    /**
     * 使得已经订阅的topic生效,此方法仅能调用一次,再次调用无效并将抛出异常
     */
    public void completeSubscribe() throws SimpleMQClientException;


    /**
     * 返回此消费者使用的offset存储器，可共享给其他消费者
     *
     * @return
     */
    public OffsetStorage getOffsetStorage();


    /**
     * 停止消费者
     */
    @Override
    public void shutdown() throws SimpleMQClientException;


    /**
     * 返回消费者配置
     *
     * @return
     */
    public ConsumerConfig getConsumerConfig();

    /**
     * Returns current RejectConsumptionHandler
     *
     * @return
     */
//    public RejectConsumptionHandler getRejectConsumptionHandler();

    /**
     * Sets RejectConsumptionHandler for this consumer.
     *
     * @param rejectConsumptionHandler
     */
//    public void setRejectConsumptionHandler(RejectConsumptionHandler rejectConsumptionHandler);

}
