package com.lin.commons;

import com.lin.commons.cluster.Partition;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午6:56
 */
public class MessageAccessor {

    public static void setId(Message message, long id) {
        message.setId(id);
    }

    public static void setPartition(Message message, Partition partition) {
        message.setPartition(partition);
    }

}
