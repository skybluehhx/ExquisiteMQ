package com.lin.server.DB;

import com.lin.commons.Message;

import java.util.List;

/**
 * 保存消息的基本接口
 *
 * @author jianglinzou
 * @date 2019/3/12 下午3:30
 */
public interface StoreMessage<T> {


    /**
     * 将消息进行保存，如果保存成功将返回消息的id
     * 如果保存失败将返回null;
     *
     * @param t
     * @return {@link Result}
     */
    public Result<String, Integer> store(T t);


    public Result<String, List<Message>> getMessage(int partition, String topic, int maxSize, long startOffset);

}
