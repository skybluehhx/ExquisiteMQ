package com.lin.client.consumer;

import com.lin.client.MetaClientConfig;
import com.lin.client.Shutdownable;
import com.lin.commons.Message;

import java.io.IOException;

/**
 * 恢复管理器
 *
 * @author jianglinzou
 * @date 2019/3/15 下午5:09
 */
public interface RecoverManager extends Shutdownable {

    /**
     * 是否已经启动
     *
     * @return
     */
    public boolean isStarted();


    /**
     * 启动recover
     *
     * @param metaClientConfig
     */
    public void start(MetaClientConfig metaClientConfig);


    /**
     * 存入一个消息
     *
     * @param group
     * @param message
     * @throws IOException
     */
    public void append(String group, Message message) throws IOException;

}
