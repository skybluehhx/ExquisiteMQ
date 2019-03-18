package com.lin.commons.utils.network;

/**
 * 自定义协议中的消息常量
 *
 * @author jianglinzou
 * @date 2019/3/11 上午11:09
 */
public class MessageContant {

    public static int messageflag = 0xabef0101;

    public static int shortMessageLength = 4 + 4 + 8 + 1 + 1 + 8;//消息的最短长度

    public static byte default_priority = 1;


}
