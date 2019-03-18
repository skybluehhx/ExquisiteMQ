package com.lin.commons.filter;

import com.lin.commons.Message;

/**
 * 消费者消息过滤器，
 * @author jianglinzou
 * @date 2019/3/8 下午6:05
 */

public interface ConsumerMessageFilter {
    /**
     * Test if the filter can accept a metaq message.Any exceptions threw by
     * this method means the message is not accepted.This method must be
     * thread-safe.
     * 如果返回true表示该消息可以被接受，如果返回false,表示该消息不会被消费者消费
     * @param group
     * @param message
     * @return true if it accepts.
     */
    public boolean accept(String group, Message message);
}
