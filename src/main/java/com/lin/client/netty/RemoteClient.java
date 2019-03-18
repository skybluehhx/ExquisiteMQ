package com.lin.client.netty;

import com.lin.client.ResponseFuture;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.utils.network.Request;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 远程客户端，当消费者需要从远程服务器获取消息时需要使用该类的实现者
 * <p>
 * 当生产者需要发送消息到远程服务器时也需要借助该接口的实现者
 *
 * @author jianglinzou
 * @date 2019/3/9 下午8:59
 */
public interface RemoteClient<T, V> {

    /**
     * 连接到远程服务器，
     *
     * @param url  远程服务器url
     * @param port 远程服务器监听的端口号
     * @param time 超时时间
     */
    public void connectRomotingServer(String url, int port, int time) throws Exception;

    /**
     * 发送消息到远程服务器的某个分区
     *
     * @param url          服务器url
     * @param nettyMessage 待发送的消息
     * @param partition    分区
     * @return NettyMessage, 有服务端返回的消息
     */
    public ResponseFuture<T, V> sendMessageToServer(String url, int port, Request nettyMessage, Partition partition) throws ExecutionException, InterruptedException, SimpleMQClientException;

    /**
     * 关闭与远程服务器的某个连接
     *
     * @param url
     * @param port
     */
    public void colseConnectToRemoteServer(String url, int port);

    /**
     * 关闭该远程客户端
     */
    public void closeClient();


    /**
     * 判断该远程呢客户端是否在还
     *
     * @param url
     * @param port
     * @return
     */
    public boolean isConnected(String url, int port);


    public void sendMessageToServer(String url, int port, Request request, Partition partition, SingleRequestCallBackListener listener, long time, TimeUnit unit) throws ExecutionException, InterruptedException, SimpleMQClientException;

}
