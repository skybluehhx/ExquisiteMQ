package com.lin.commons;

import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;

import java.util.List;

/**
 * @author jianglinzou
 * @date 2019/3/8 下午6:18
 */
public interface PartitionSelector {

    /**
     * 根据topic、message从partitions列表中选择分区
     *
     * @param topic
     *            topic
     * @param partitions
     *            分区列表
     * @param message
     *            消息
     * @return
     * @throws SimpleMQClientException
     *             此方法抛出的任何异常都应当包装为SimpleMQClientException
     */
    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws SimpleMQClientException;


}
