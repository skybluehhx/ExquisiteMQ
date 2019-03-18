package com.lin.commons.utils.codec;

import java.io.IOException;

/**
 * 序列化对象接口
 *
 * @author jianglinzou
 * @date 2019/3/8 下午6:22
 */
public interface SerializerObject<T> {

    /**
     * 将指定的字节码反序列化为对象
     *
     * @param in - 指定的字节码内容
     * @return - 返回反序列化后的对象
     */
    public T decodeObject(byte[] in) throws IOException;


    /**
     * 将指定的对象进行序列化为字节
     *
     * @param obj - 需要序列化的对象
     * @return - 返回对象序列化后的字节码
     */
    public byte[] encodeObject(T obj) throws IOException;


}
