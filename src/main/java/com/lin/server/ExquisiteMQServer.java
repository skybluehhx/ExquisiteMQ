package com.lin.server;

/**
 * @author jianglinzou
 * @date 2019/3/14 下午2:44
 */
public interface ExquisiteMQServer {


    /**
     * 启动消息队列服务器
     */
    public void start() throws Exception;


    /**
     * 关闭消息队列服务器
     */
    public void stop();


}
