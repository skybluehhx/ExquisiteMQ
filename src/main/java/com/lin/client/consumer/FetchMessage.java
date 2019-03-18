package com.lin.client.consumer;

import com.lin.commons.Message;
import com.lin.commons.MessageIterator;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.filter.ConsumerMessageFilter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 从服务器抓取消息的接口
 *
 * @author jianglinzou
 * @date 2019/3/15 下午6:05
 */
public interface FetchMessage {


    /**
     * 抓取消息
     *
     * @param fetchRequest
     * @param timeout
     * @param timeUnit
     * @return
     * @throws SimpleMQClientException
     * @throws InterruptedException
     */
    MessageIterator fetch(final FetchRequest fetchRequest, long timeout, TimeUnit timeUnit) throws SimpleMQClientException,
            InterruptedException;


    /**
     * 处理无法被客户端消费的消息
     *
     * @param message
     * @throws IOException
     */
    void appendCouldNotProcessMessage(final Message message) throws IOException;


    /**
     * 查询offset,可从zk上查询
     *
     * @param fetchRequest
     * @return
     * @throws SimpleMQClientException
     */
    long offset(final FetchRequest fetchRequest) throws SimpleMQClientException;


    /**
     * 返回topic对应的消息监听器
     *
     * @param topic
     * @return
     */
    MessageListener getMessageListener(final String topic);


    public ConsumerMessageFilter getMessageFilter(final String topic);


    public ConsumerConfig getConsumerConfig();
}
