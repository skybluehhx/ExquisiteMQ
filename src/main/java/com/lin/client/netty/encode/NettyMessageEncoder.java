package com.lin.client.netty.encode;

import com.lin.commons.utils.codec.SerializerObject;
import com.lin.commons.utils.network.Transportation;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author jianglinzou
 * @date 2019/3/11 上午10:59
 */
public class NettyMessageEncoder extends MessageToByteEncoder<Transportation> {

    public static Logger logger = LoggerFactory.getLogger(NettyMessageEncoder.class);
    private SerializerObject serializerObject;

    public NettyMessageEncoder(SerializerObject serializerObject) {
        this.serializerObject = serializerObject;
    }

    private static final byte[] LENGTH_PLACHOLDER = new byte[4];

    protected void encode(ChannelHandlerContext ctx, Transportation msg, ByteBuf out) throws Exception {

        if (msg == null || msg.getHeader() == null) {
            throw new Exception("the encode message is null");
        }
        logger.info("客户端编码到服务端" + msg.getHeader().toString() + msg.getBody());
        ByteBuf sendBuf = out;
        sendBuf.writeInt((msg.getHeader().getCroCode()));
        sendBuf.writeLong((msg.getHeader().getSessionID()));
        sendBuf.writeByte((msg.getHeader().getType()));
        sendBuf.writeByte((msg.getHeader().getReq()));
        sendBuf.writeLong(msg.getHeader().getRequestId());
        if (msg.getBody() != null) {
            byte[] bytes = encodeObject(msg.getBody());
            msg.getHeader().setLength(bytes.length);
            sendBuf.writeInt((msg.getHeader().getLength()));
            sendBuf.writeBytes(bytes);

        } else {
            msg.getHeader().setLength(0);
            sendBuf.writeInt((msg.getHeader().getLength()));
        }
    }

    public byte[] encodeObject(Object msg) throws IOException {
        byte[] objectBytes = this.serializerObject.encodeObject(msg);
        return objectBytes;
    }

    public SerializerObject getSerializerObject() {
        return serializerObject;
    }

    public void setSerializerObject(SerializerObject serializerObject) {
        this.serializerObject = serializerObject;
    }

    public static void main(String[] args) {
        System.out.println("你好");
    }
}
