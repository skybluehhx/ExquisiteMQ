package com.lin.client.consumer.storage;

import com.lin.client.consumer.TopicPartitionRegInfo;
import com.lin.client.consumer.ZkClientChangedListener;
import com.lin.commons.cluster.Partition;
import com.lin.commons.utils.ZkUtils;
import com.lin.commons.utils.zk.MetaZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午5:10
 */
public class ZkOffsetStorage implements OffsetStorage, ZkClientChangedListener {

    private volatile ZkClient zkClient;
    private final MetaZookeeper metaZookeeper;


    @Override
    public void onZkClientChanged(final ZkClient newClient) {
        log.info("Update ZkOffsetStorage's zkClient...");
        this.zkClient = newClient;
    }


    public ZkOffsetStorage(final MetaZookeeper metaZookeeper, final ZkClient zkClient) {
        super();
        this.metaZookeeper = metaZookeeper;
        this.zkClient = zkClient;
    }

    static final Log log = LogFactory.getLog(ZkOffsetStorage.class);


    @Override
    public void commitOffset(final String group, final Collection<TopicPartitionRegInfo> infoList) {
        if (this.zkClient == null || infoList == null || infoList.isEmpty()) {
            return;
        }
        for (final TopicPartitionRegInfo info : infoList) {
            final String topic = info.getTopic();
            final MetaZookeeper.ZKGroupTopicDirs topicDirs = this.metaZookeeper.new ZKGroupTopicDirs(topic, group);
            long newOffset = -1;
            long msgId = -1;
            // 加锁，保证msgId和offset一致
            synchronized (info) {
                // 只更新有变更的
                if (!info.isModified()) {
                    continue;
                }
                newOffset = info.getOffset().get();
                msgId = info.getMessageId();
                // 更新完毕，设置为false
                info.setModified(false);
            }
            try {
                // 存储到zk里的数据为msgId-offset
                // 原始只有offset，从1.4开始修改为msgId-offset,为了实现同步复制
                ZkUtils.updatePersistentPath(this.zkClient, topicDirs.consumerOffsetDir + "/"
                        + info.getPartition().toString(), msgId + "-" + newOffset);
            }
            catch (final Throwable t) {
                log.error("exception during commitOffsets", t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Committed offset " + newOffset + " for topic " + info.getTopic());
            }

        }
    }

    //meta/consumers/group(具体的组)/ids/offsets/topic(具体的topic)/partition
    @Override
    public TopicPartitionRegInfo load(final String topic, final String group, final Partition partition) {
        final MetaZookeeper.ZKGroupTopicDirs topicDirs = this.metaZookeeper.new ZKGroupTopicDirs(topic, group);
        final String znode = topicDirs.consumerOffsetDir + "/" + partition.toString();
        final String offsetString = ZkUtils.readDataMaybeNull(this.zkClient, znode);
        if (offsetString == null) {
            return null;
        }
        else {
            // 兼容老客户端
            final int index = offsetString.lastIndexOf("-");
            if (index > 0) {
                // 1.4开始的新客户端,读取已消费的offset
                final long msgId = Long.parseLong(offsetString.substring(0, index));
                final long offset = Long.parseLong(offsetString.substring(index + 1));
                return new TopicPartitionRegInfo(topic, partition, offset, msgId);
            }
            else {
                // 老客户端
                final long offset = Long.parseLong(offsetString);
                return new TopicPartitionRegInfo(topic, partition, offset);
            }
        }
    }


    @Override
    public void close() {
        // do nothing
    }


    @Override
    public void initOffset(final String topic, final String group, final Partition partition, final long offset) {
        // do nothing
    }
}
