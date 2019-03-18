package com.lin.client.consumer;

import org.I0Itec.zkclient.ZkClient;

/**
 *
 * zkClient监听器，当客户端与zk断开重连的时候使用
 * @author jianglinzou
 * @date 2019/3/11 下午1:26
 */
public interface ZkClientChangedListener {

    /**
     * 当新的zkClient建立的时候
     *
     * @param newClient
     */
    public void onZkClientChanged(ZkClient newClient);
}
