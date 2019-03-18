package com.lin.client.consumer;

import com.lin.commons.Message;

import java.util.concurrent.Executor;

/**
 * 异步消息监听器
 * @author jianglinzou
 * @date 2019/3/11 下午1:25
 */
public interface MessageListener {


    /**
     * 接收到消息，只有messages不为空并且不为null的情况下会触发此方法
     *
     * @param messages
     *            TODO 拼写错误，应该是单数，暂时将错就错吧
     */
    public void recieveMessages(Message message) throws InterruptedException;


    /**
     * 处理消息的线程池
     *
     * @return
     */
    public Executor getExecutor();
}
