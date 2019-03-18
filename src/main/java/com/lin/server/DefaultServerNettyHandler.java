package com.lin.server;

import com.alibaba.fastjson.JSONObject;
import com.lin.commons.Message;
import com.lin.commons.utils.JSONUtils;
import com.lin.commons.utils.network.Header;
import com.lin.commons.utils.network.MessageTypeContant;
import com.lin.commons.utils.network.Request;
import com.lin.commons.utils.network.Response;
import com.lin.server.DB.JDBCStoreMessage;
import com.lin.server.DB.Result;
import com.lin.server.DB.StoreMessage;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static com.lin.commons.utils.network.MessageTypeContant.GET_MESSAGE;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午4:11
 */
public class DefaultServerNettyHandler extends ChannelHandlerAdapter {

    public static Logger logger = LoggerFactory.getLogger(DefaultNettyServer.class);
    // 用于获取客户端发送的信息

    StoreMessage storeMessage = new JDBCStoreMessage();

    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        // 用于获取客户端发来的数据信息
        Request request = (Request) msg;
        logger.info("Server接受的客户端的信息 :" + request.toString());
        Response response = handlerMessageFromClient(request);
        if (Objects.nonNull(response)) {
            // 回写数据给客户端
            ctx.writeAndFlush(response);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        cause.printStackTrace();
        ctx.close();
    }


    /**
     * todo
     * 处理从客户端传过来的消息
     */
    private Response handlerMessageFromClient(Request recMeg) {
        if (recMeg == null) {
            return null;
        }
// TODO: 2019/3/13
        Header header = recMeg.getHeader();
        logger.info("get message from client ,the message:{}", recMeg.getBody());
        if (header.getType() == MessageTypeContant.SEND_MESSAGE) {
            return handlerWhenProducer(recMeg);
        }
        if (header.getType() == GET_MESSAGE) {
            logger.info("写回的数据");
            return handlerWhenConsumer(recMeg);

        }
        return null;

    }


    private Response handlerWhenConsumer(Request recMeg) {
        Response response = new Response();
        JSONObject jsonObject = JSONUtils.getJSONObject(recMeg.getBody());
        long currentOffset = jsonObject.getLong("currentOffset");
        int size = jsonObject.getInteger("size");
        String group = jsonObject.getString("group");
        int brokerId = jsonObject.getInteger("brokerId");
        String topic = jsonObject.getString("topic");
        int partition = jsonObject.getInteger("partition");
        logger.info("the brokerId:{} request message of partition :{} ,the size is :{},the group:{}", brokerId, partition, size, group);
        Result<String, List<Message>> result = storeMessage.getMessage(partition, topic, size, currentOffset);
        Header backHeader = new Header();
        Header header = recMeg.getHeader();
        backHeader.setSessionID(header.getSessionID());
        backHeader.setType(MessageTypeContant.RESP_GET_MESSAGE);
        backHeader.setRequestId(header.getRequestId());
        response.setHeader(header);
        JSONObject resultJson = new JSONObject();
        if (result.isSuccess()) {
            logger.info("读取消息成功，the message is :{}", result.getSuccess());
            resultJson.put("message", result.getSuccess());
            resultJson.put("topic", topic);
        } else {
            logger.info("读取消息失败，the message is :{}", result.getError());
            resultJson.put("error", result.getError());
        }
        response.setBody(JSONUtils.toJsonString(resultJson));
        return response;
    }

    private Response handlerWhenProducer(Request recMeg) {

        //生成消息id,落库保存后返回消息id
        logger.info("start to store message");
        Message message = JSONUtils.getObject(recMeg.getBody(), Message.class);
        if (Objects.nonNull(message)) {
            Header header = recMeg.getHeader();
            Result<String, Integer> result = storeMessage.store(message);
            logger.info("store message success,will returen response for requestId:{}", header.getRequestId());
            Response response = new Response();
            Header backHeader = new Header();
            backHeader.setSessionID(header.getSessionID());
            backHeader.setType(MessageTypeContant.RESP_SEND_MESSAGE);
            backHeader.setRequestId(header.getRequestId());
            response.setHeader(backHeader);
            JSONObject resultJson = new JSONObject();
            if (result.isSuccess()) {
                resultJson.put("messageId", result.getSuccess());
            } else {
                resultJson.put("messageId", -1);
                resultJson.put("error", result.getError());
            }
            response.setBody(JSONUtils.toJsonString(resultJson));
            //保存到数据库中
            return response;
        } else {
            logger.error("receive error data type");
            return null;
        }


    }

    public StoreMessage getStoreMessage() {
        return storeMessage;
    }

    public void setStoreMessage(StoreMessage storeMessage) {
        this.storeMessage = storeMessage;
    }
}
