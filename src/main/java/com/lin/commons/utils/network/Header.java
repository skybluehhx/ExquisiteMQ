package com.lin.commons.utils.network;

import com.lin.commons.utils.IdGenerator;
import lombok.Data;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jianglinzou
 * @date 2019/3/11 上午10:57
 */

@Data
@ToString
public class Header {

    //消息开头的标志
    private int croCode = MessageContant.messageflag;

    private long sessionID; //回话id

    private byte type; //消息类型

    private byte req; //1表示是请求头部，0表示是响应头部

    private int length;//消息的长度

    private long requestId;//用于标识

    public Header() {

    }


    public static void main(String[] args) {

    }

//    private Map<String, Object> attachment = new HashMap<String, Object>(); //附件


}
