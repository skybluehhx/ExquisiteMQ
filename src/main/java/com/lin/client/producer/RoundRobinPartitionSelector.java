package com.lin.client.producer;

import com.lin.commons.Message;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.utils.PositiveAtomicCounter;

import java.util.List;

/**
 * 轮询的分区选择器，默认使用此选择器
 * @author jianglinzou
 * @date 2019/3/11 下午1:20
 */
public class RoundRobinPartitionSelector extends AbstractPartitionSelector {

    private final PositiveAtomicCounter sets = new PositiveAtomicCounter();



    public Partition getPartition0(final String topic, final List<Partition> partitions, final Message message)
            throws SimpleMQClientException {
        try {
            return partitions.get(this.sets.incrementAndGet() % partitions.size());
        }
        catch (final Throwable t) {
            throw new SimpleMQClientException(t);
        }
    }
}
