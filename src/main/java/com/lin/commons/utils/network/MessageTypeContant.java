package com.lin.commons.utils.network;

/**
 * 消息类型常量，消息有多种，
 * 有些是检测的心跳信息
 * 服务器回送心跳的信息
 * 有些是正常发送的消息
 * 服务器回复正常发送的消息
 *
 * @author jianglinzou
 * @date 2019/3/11 下午3:09
 */
public interface MessageTypeContant {


    public static byte SEND_MESSAGE = 1;//表示正常发送的类型，服务端需要将该消息落库保存


    public static byte RESP_SEND_MESSAGE = 2;//表示服务器对正常发送消息的应答类型
    public static byte GET_MESSAGE = 3; //表示消费者正常从服务器中拉取消息
    public static byte RESP_GET_MESSAGE = 4;//表示服务器正常返回给消费者消息

}
