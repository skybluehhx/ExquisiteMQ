package com.lin.client.producer;

import com.lin.commons.Message;
import com.lin.commons.PartitionSelector;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;

import java.util.List;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:18
 */
public abstract class AbstractPartitionSelector implements PartitionSelector {


    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws SimpleMQClientException {
        if (partitions == null) {
            throw new SimpleMQClientException("There is no aviable partition for topic " + topic
                    + ",maybe you don't publish it at first?");
        }
        return this.getPartition0(topic, partitions, message);
    }


    public abstract Partition getPartition0(String topic, List<Partition> partitions, Message message)
            throws SimpleMQClientException;
}
