package com.lin.client.netty.impl;

import com.lin.client.ResponseFuture;
import com.lin.client.netty.RemoteClient;
import com.lin.client.netty.encode.NettyMessageDecoder;
import com.lin.client.netty.encode.NettyMessageEncoder;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.utils.codec.SerializerFactory;
import com.lin.commons.utils.network.Request;
import com.lin.commons.utils.network.Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午3:51
 */
public class DefaultRemoteClient implements RemoteClient {

    private static Logger logger = LoggerFactory.getLogger(DefaultRemoteClient.class);


    private EventLoopGroup group = new NioEventLoopGroup();

    private Bootstrap b = new Bootstrap();


    //维持当前客户端已连接的channel,主要由于broker失效，客户端需要选择broker重新连接
    ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();

    ConcurrentHashMap<String, FutureTask<Boolean>> futuretasks = new ConcurrentHashMap<>();

    ConcurrentHashMap<Long, ResponseFuture<Response, Request>> responses = new ConcurrentHashMap<>();

    public void connectRomotingServer(String url, int port, int time) throws Exception {
        connect(port, url, time);
    }

    public ResponseFuture<Response, Request> sendMessageToServer(String url, int port, Request request, Partition partition) throws InterruptedException, ExecutionException, SimpleMQClientException {
        System.out.println("53 defaultRemoteClient start to send message");
        FutureTask<Boolean> connectTask = futuretasks.get(generatekey(url, port));//确保在发送消息到服务器时连接已经建立完成；
        connectTask.get();//确保连接已经建立完成
        Channel channel = channels.get(generatekey(url, port));
        if (Objects.isNull(channel) || !channel.isOpen()) {
            throw new SimpleMQClientException("服务器通道异常关闭");
        }
        if (Objects.isNull(request) || Objects.isNull(request.getHeader())) {
            logger.error("the data is error ,please ensure request and request's header not null ");
            throw new SimpleMQClientException("the data is error ,please ensure request and request's header not null");
        }
        ResponseFuture<Response, Request> responseFuture = new ResponseFuture<>();
        responseFuture.setRequest(request);
        channel.writeAndFlush(request);
        responses.put(request.getHeader().getRequestId(), responseFuture);
        return responseFuture;
    }


    @Override
    public void colseConnectToRemoteServer(String url, int port) {
        Channel channel = channels.remove(generatekey(url, port));
        if (Objects.isNull(channel) || !channel.isOpen()) {
            return;
        }
        channel.close();

    }

    @Override
    public void closeClient() {
        if (Objects.nonNull(group)) {
            try {
                group.shutdownGracefully();
            } finally {
                logger.info("the client has been shutdown");
            }

        }
    }

    @Override
    public boolean isConnected(String url, int port) {
        Channel channel = channels.get(generatekey(url, port));
        if (Objects.isNull(channel) || !channel.isOpen()) {
            return false;
        }
        return true;
    }


    public void connect(int port, String host, int time) {
        Channel channel = channels.get(generatekey(host, port));
        if (Objects.nonNull(channel) && !channel.isOpen()) { //存在重连的情况，移除先前的关闭的channel
            channel.close();
            channels.remove(host, port);
        }
        //已经连接
        if (Objects.nonNull(channel) && channel.isOpen()) {
            return;
        }
        // 配置客户端NIO线程组
        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(() -> {
            b.group(group)
                    .channel(NioSocketChannel.class)//
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, time)//
                    .handler(new MyChannelHandler());//
            // 异步链接服务器 同步等待链接成功
            ChannelFuture f = b.connect(host, port).sync();
            logger.info("success to connect server the host:{},the port:{}", host, port);
            channels.put(generatekey(host, port), f.channel());
            return true;
        });
        futuretasks.put(generatekey(host, port), futureTask);
        futureTask.run();//

    }


    /**
     * 网络事件处理器
     */
    private class MyChannelHandler extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            // 添加自定义协议的编解码工具
            ch.pipeline().addLast(new NettyMessageEncoder(SerializerFactory.buildSerializer(SerializerFactory.Codec_Type.HESSIAN)));
            ch.pipeline().addLast(new NettyMessageDecoder(SerializerFactory.buildSerializer(SerializerFactory.Codec_Type.HESSIAN)));
            // 处理网络IO
            ch.pipeline().addLast(new DefaultRemoteClientHandler());
        }

    }


    class DefaultRemoteClientHandler extends ChannelHandlerAdapter {

        // 客户端与服务端，连接成功的售后
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("success to connect :{}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception {
            try {
                Response response = (Response) msg;
                logger.info("接收到服务端的信息，the message's requestId is " + response.getHeader().getRequestId());
                ResponseFuture<Response, Request> future = responses.get(response.getHeader().getRequestId());
                if (Objects.nonNull(future)) {
                    future.setResponse(response);
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            ctx.close();
        }

    }


    private String generatekey(String host, int port) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(host).append(":").append(port);
        return stringBuilder.toString();
    }

}



