package com.lin.client;

import com.lin.commons.Message;
import com.lin.commons.utils.JSONUtils;
import com.lin.commons.utils.LongSequenceGenerator;
import com.lin.commons.utils.network.Header;
import com.lin.commons.utils.network.MessageTypeContant;
import com.lin.commons.utils.network.Request;

import static com.lin.commons.utils.network.MessageContant.default_priority;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午3:05
 */
public class MessageUtils {
//    static AtomicLong atomicLong = new AtomicLong(0);

    /**
     * 生成个request时，需要为该request在本机上设置全局id
     * <p>
     * 将相对于客户端而言message包装在netty中传输的通用消息体
     *
     * @param message   具体的小消息
     * @param sessionID 回话id
     * @param type      消息类型
     * @param priority  消息优先级
     * @return
     */
    public static Request generateRequest(Message message, long sessionID, byte type, byte priority) {

        Request request = new Request();
        Header header = new Header();
        header.setSessionID(sessionID);
        header.setType(type);
        request.setHeader(header);
        request.getHeader().setRequestId(LongSequenceGenerator.getNextSequenceId());
        String content = JSONUtils.toJsonString(message);
        request.setBody(content);
        return request;

    }


    public static Request generateRequest(Message message, long sessionID) {
        return generateRequest(message, sessionID, MessageTypeContant.SEND_MESSAGE, default_priority);
    }


}
