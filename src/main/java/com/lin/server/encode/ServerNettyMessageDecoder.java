package com.lin.server.encode;

import com.lin.commons.utils.codec.SerializerObject;
import com.lin.commons.utils.network.Header;
import com.lin.commons.utils.network.MessageContant;
import com.lin.commons.utils.network.Request;
import com.lin.commons.utils.network.Transportation;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * 服务端解码器
 * @author jianglinzou
 * @date 2019/3/13 下午2:00
 */
public class ServerNettyMessageDecoder  extends ByteToMessageDecoder {

    private SerializerObject serializerObject;

    public ServerNettyMessageDecoder(SerializerObject serializerObject) {

        this.serializerObject = serializerObject;
    }


    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        // 可读长度必须大于基本长度
        if (buffer.readableBytes() >= MessageContant.shortMessageLength) {
            // 防止socket字节流攻击
            // 防止，客户端传来的数据过大
            // 因为，太大的数据，是不合理的
            if (buffer.readableBytes() > 2048) {
                buffer.skipBytes(buffer.readableBytes());
            }

            // 记录包头开始的index
            int beginReader;

            while (true) {
                // 获取包头开始的index
                beginReader = buffer.readerIndex();
                // 标记包头开始的index
                buffer.markReaderIndex();
                // 读到了协议的开始标志，结束while循环
                if (buffer.readInt() == MessageContant.messageflag) {
                    break;
                }

                // 未读到包头，略过一个字节
                // 每次略过，一个字节，去读取，包头信息的开始标记
                buffer.resetReaderIndex();
                buffer.readByte();

                // 当略过，一个字节之后，
                // 数据包的长度，又变得不满足
                // 此时，应该结束。等待后面的数据到达
                if (buffer.readableBytes() < MessageContant.shortMessageLength) {
                    return;
                }
            }

            // 消息的长度

            long sessionId = buffer.readLong();
            byte type = buffer.readByte();
            byte isReq = buffer.readByte();
            long reqId = buffer.readLong();
            int contentLength = buffer.readInt();
            // 判断请求数据包数据是否到齐
            if (buffer.readableBytes() < contentLength) {
                // 还原读指针
                buffer.readerIndex(beginReader);
                return;
            }
            // 读取data数据
            byte[] data = new byte[contentLength];
            buffer.readBytes(data);
//
            Transportation transportation = new Request();
            Header header = new Header();
            header.setCroCode(MessageContant.messageflag);
            header.setSessionID(sessionId);
            header.setType(type);
            header.setReq(isReq);
            header.setRequestId(reqId);
            header.setLength(contentLength);

            transportation.setHeader(header);
            Object o = serializerObject.decodeObject(data);
            transportation.setBody(o.toString());
            out.add(transportation);
        }
    }

    public SerializerObject getSerializerObject() {
        return serializerObject;
    }

    public void setSerializerObject(SerializerObject serializerObject) {
        this.serializerObject = serializerObject;
    }
}
