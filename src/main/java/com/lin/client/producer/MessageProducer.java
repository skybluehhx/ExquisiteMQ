package com.lin.client.producer;

import com.lin.client.Shutdownable;
import com.lin.commons.Message;
import com.lin.commons.PartitionSelector;
import com.lin.commons.exception.SimpleMQClientException;

import java.util.concurrent.TimeUnit;

/**
 * 消息生产者，线程安全，推荐复用
 * @author jianglinzou
 * @date 2019/3/11 下午1:18
 */
public interface MessageProducer extends Shutdownable {

    /**
     * 发布topic，以便producer从zookeeper获取broker列表并连接，在发送消息前必须先调用此方法
     *  会在zk下创建相应的路径，并监听该topic下broker的变化，以便达到发送的负载均衡
     * @param topic
     */
    public void publish(String topic);


    /**
     * 设置发送消息的默认topic，当发送的message的topic没有找到可用broker和分区的时候，选择这个默认topic指定的broker发送
     * 。调用本方法会自动publish此topic。
     *
     * @param topic
     */
    public void setDefaultTopic(String topic);


    /**
     * 发送消息
     *
     * @param message
     *            消息对象
     *
     * @return 发送结果
     * @throws SimpleMQClientException
     *             客户端异常
     * @throws InterruptedException
     *             响应中断
     */
    public SendResult sendMessage(Message message) throws SimpleMQClientException, InterruptedException;


    /**
     * 异步发送消息，在指定时间内回调callback,此模式下无法使用事务
     *
     * @param message
     * @param cb
     * @param time
     * @param unit
     * @since 1.4
     */
    public void sendMessage(Message message, SendMessageCallback cb, int time, TimeUnit unit);


    /**
     * 异步发送消息，在默认时间内（3秒）回调callback，此模式下无法使用事务
     *
     * @param message
     * @param cb
     * @since 1.4
     */
    public void sendMessage(Message message, SendMessageCallback cb);


    /**
     * 发送消息,如果超出指定的时间内没有返回，则抛出异常
     *
     * @param message
     *            消息对象
     * @param timeout
     *            超时时间
     * @param unit
     *            超时的时间单位
     * @return 发送结果
     * @throws SimpleMQClientException
     *             客户端异常
     * @throws InterruptedException
     *             响应中断
     */
    public SendResult sendMessage(Message message, int timeout, TimeUnit unit) throws SimpleMQClientException,
            InterruptedException;


    /**
     * 关闭生产者，释放资源
     */
    public void shutdown() throws SimpleMQClientException;


    /**
     * 返回本生产者的分区选择器
     *
     * @return
     */
    public PartitionSelector getPartitionSelector();


    /**
     * 返回本生产者发送消息是否有序,这里的有序是指发往同一个partition的消息有序。此方法已经废弃，总是返回false
     *
     * @return true表示有序
     */
    @Deprecated
    public boolean isOrdered();


    /**
     * 开启一个事务并关联到当前线程，在事务内发送的消息将作为一个单元提交给服务器，要么全部发送成功，要么全部失败
     *
     * @throws SimpleMQClientException
     *             如果已经处于事务中，则抛出TransactionInProgressException异常
     */
//    public void beginTransaction() throws SimpleMQClientException;


    /**
     * 设置事务超时时间，从事务开始计时，如果超过设定时间还没有提交或者回滚，则服务端将无条件回滚该事务。
     *
     * @param seconds
     *            事务超时时间，单位：秒
     * @throws SimpleMQClientException
     * @see #beginTransaction()
     * @see #rollback()
     * @see #commit()
     */
//    public void setTransactionTimeout(int seconds) throws SimpleMQClientException;


    /**
     * Set transaction command request timeout.default is five seconds.
     *
     * @param time
     * @param timeUnit
     */
//    public void setTransactionRequestTimeout(long time, TimeUnit timeUnit);


    /**
     * 返回当前设置的事务超时时间，默认为0,表示永不超时
     *
     * @return 事务超时时间，单位：秒
     * @throws SimpleMQClientException
     */
//    public int getTransactionTimeout() throws SimpleMQClientException;


    /**
     * 回滚当前事务内所发送的任何消息，此方法仅能在beginTransaction之后调用
     *
     * @throws SimpleMQClientException
     * @see #beginTransaction()
     */
//    public void rollback() throws SimpleMQClientException;


    /**
     * 提交当前事务，将事务内发送的消息持久化，此方法仅能在beginTransaction之后调用
     *
     * @see #beginTransaction()
     * @throws SimpleMQClientException
     */
//    public void commit() throws SimpleMQClientException;
//
}
