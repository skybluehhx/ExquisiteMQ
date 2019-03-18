package com.lin.client.producer;

import com.lin.commons.Message;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.SimpleMQClientException;

import java.util.List;
import java.util.Random;

/**
 * Random partition selector
 * @author jianglinzou
 * @date 2019/3/11 下午1:20
 */
public class RandomPartitionSelector extends AbstractPartitionSelector {

    final Random rand = new Random();


    @Override
    public Partition getPartition0(String topic, List<Partition> partitions, Message message)
            throws SimpleMQClientException {
        return partitions.get(this.rand.nextInt(partitions.size()));
    }
}
