package com.lin.client.consumer.storage;

import com.lin.client.consumer.TopicPartitionRegInfo;
import com.lin.commons.cluster.Partition;

import java.util.Collection;

/**
 * Offset存储器接口
 *
 * @author jianglinzou
 * @date 2019/3/11 下午12:09
 */
public interface OffsetStorage {

        /**
         * 保存offset到存储
         *
         * @param group
         *            消费者组名
         * @param infoList
         *            消费者订阅的消息分区信息列表
         */
        public void commitOffset(String group, Collection<TopicPartitionRegInfo> infoList);


        /**
         * 加载一条消费者的订阅信息，如果不存在返回null
         *
         * @param topic
         * @param group
         * @param partition
         * @return
         */
        public TopicPartitionRegInfo load(String topic, String group, Partition partition);


        /**
         * 释放资源，meta客户端在关闭的时候会主动调用此方法
         */
        public void close();


        /**
         * 初始化offset
         *
         * @param topic
         * @param group
         * @param partition
         * @param offset
         */
        public void initOffset(String topic, String group, Partition partition, long offset);


}
