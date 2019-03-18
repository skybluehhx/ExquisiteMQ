package com.lin.client;

import com.lin.client.consumer.ConsumerConfig;
import com.lin.client.consumer.MessageConsumer;
import com.lin.client.consumer.storage.OffsetStorage;
import com.lin.client.producer.MessageProducer;
import com.lin.commons.PartitionSelector;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午9:36
 */
public interface MessageSessionFactory extends Shutdownable {

    /**
     * 关闭工厂
     *
     * @throws SimpleMQClientException
     */

    public void shutdown() throws SimpleMQClientException;


    /**
     * 创建消息生产者
     *
     * @param partitionSelector
     *            分区选择器
     * @return
     */
    public MessageProducer createProducer(PartitionSelector partitionSelector);


    /**
     * 创建消息生产者，默认使用轮询分区选择器
     *
     * @return
     */
    public MessageProducer createProducer();


    /**
     * 创建消息生产者，默认使用轮询分区选择器。本方法已经废弃，请勿使用，不排除在未来某个版本删除。
     *
     * @param ordered
     *            是否有序，true为有序，如果有序，则消息按照发送顺序保存在MQ server
     * @return
     */
    @Deprecated
    public MessageProducer createProducer(boolean ordered);


    /**
     * 创建消息生产者,本方法已经废弃，请勿使用，不排除在未来某个版本删除。
     *
     * @param partitionSelector
     *            分区选择器
     * @param ordered
     *            是否有序，true为有序，如果有序，则消息按照发送顺序保存在MQ server
     * @return
     */
    @Deprecated
    public MessageProducer createProducer(PartitionSelector partitionSelector, boolean ordered);


    /**
     * 创建消息消费者，默认将offset存储在zk
     *
     * @param consumerConfig
     *            消费者配置
     * @return
     */
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig);


    /**
     * Get statistics information from all brokers in this session factory.
     *
     * @return statistics result
     * @since 1.4.2
     */
//    public Map<InetSocketAddress, StatsResult> getStats() throws InterruptedException;


    /**
     * Get item statistics information from all brokers in this session factory.
     *
     * @param item
     *            stats item,could be "topics","realtime","offsets" or a special
     *            topic
     * @return statistics result
     * @since 1.4.2
     */
//    public Map<InetSocketAddress, StatsResult> getStats(String item) throws InterruptedException;


    /**
     * Get statistics information from special broker.If the broker is not
     * connected in this session factory,it will return null.
     *
     * @param target
     *            stats broker
     * @return statistics result
     * @since 1.4.2
     */
//    public StatsResult getStats(InetSocketAddress target) throws InterruptedException;


    /**
     * Get item statistics information from special broker.If the broker is not
     * connected in this session factory,it will return null.
     *
     * @param target
     *            stats broker
     * @param item
     *            stats item,could be "topics","realtime","offsets" or a special
     *            topic
     * @return statistics result
     * @since 1.4.2
//     */
//    public StatsResult getStats(InetSocketAddress target, String item) throws InterruptedException;


    /**
     * 创建消息消费者，使用指定的offset存储器
     *
     * @param consumerConfig
     *            消费者配置
     * @param offsetStorage
     *            offset存储器
     * @return
     */
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig, OffsetStorage offsetStorage);


    /**
     * Get partitions list for topic
     *
     * @param topic
     * @return partitions list
     */
    public List<Partition> getPartitionsForTopic(String topic);


    /**
     * Returns a topic browser to iterate all messages under the topic from all
     * alive brokers.
     *
     * @param topic
     *            the topic
     * @param maxSize
     *            fetch batch size in bytes.
     * @param timeout
     *            timeout value to fetch messages.
     * @param timeUnit
     *            timeout value unit.
     * @since 1.4.5
     * @return topic browser
     */
//    public TopicBrowser createTopicBrowser(String topic, int maxSize, long timeout, TimeUnit timeUnit);


    /**
     * Returns a topic browser to iterate all messages under the topic from all
     * alive brokers.
     *
     * @param topic
     *            the topic
     * @since 1.4.5
     * @return topic browser
     * @see #createTopicBrowser(String, int, long, TimeUnit)
     */
//    public TopicBrowser createTopicBrowser(String topic);

}
