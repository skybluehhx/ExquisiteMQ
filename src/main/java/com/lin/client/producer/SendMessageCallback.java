package com.lin.client.producer;

/**
 * 发送消息的回调
 * @author jianglinzou
 * @date 2019/3/11 下午1:21
 */
public interface SendMessageCallback {

    /**
     * 当消息发送返回后回调，告知发送结果
     *
     * @param result
     *            发送结果
     */
    public void onMessageSent(SendResult result);


    /**
     * 当发生异常的时候回调本方法
     *
     * @param e
     */
    public void onException(Throwable e);
}
