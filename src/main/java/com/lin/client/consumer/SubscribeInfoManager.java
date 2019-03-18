package com.lin.client.consumer;

import com.lin.commons.exception.SimpleMQClientException;
import com.lin.commons.filter.ConsumerMessageFilter;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 对客户端订阅信息的管理，
 * 同一进程内，只能有一个组内只有一个消费者对一个topic进行订阅
 * @author jianglinzou
 * @date 2019/3/15 下午5:17
 */
public class SubscribeInfoManager {

    private final ConcurrentHashMap<String/* group */, ConcurrentHashMap<String/* topic */, SubscriberInfo>> groupTopicSubcriberRegistry =
            new ConcurrentHashMap<String/* group */, ConcurrentHashMap<String, SubscriberInfo>>();

    //在同一个进程中，确保topic不被同一组重复订阅，->表明一个进程中，每组只能被一个线程消费
    public void subscribe(final String topic, final String group, final int maxSize,
                          final MessageListener messageListener, final ConsumerMessageFilter consumerMessageFilter)
            throws SimpleMQClientException {
        final ConcurrentHashMap<String, SubscriberInfo> topicSubsriberRegistry = this.getTopicSubscriberRegistry(group);
        SubscriberInfo info = topicSubsriberRegistry.get(topic);  // 1
        if (info == null) {
            info = new SubscriberInfo(messageListener, consumerMessageFilter, maxSize);
            //在加一次判断，确保确实没有，因为没加锁，可能存在多线程问题，两个线程以前都没订阅过，但两个线程同时执行到 1
            final SubscriberInfo oldInfo = topicSubsriberRegistry.putIfAbsent(topic, info);
            if (oldInfo != null) {
                throw new SimpleMQClientException("Topic=" + topic + " has been subscribered by group " + group);
            }
        }
        else {
            throw new SimpleMQClientException("Topic=" + topic + " has been subscribered by group " + group);
        }
    }


    private ConcurrentHashMap<String, SubscriberInfo> getTopicSubscriberRegistry(final String group)
            throws SimpleMQClientException {
        ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubsriberRegistry =
                this.groupTopicSubcriberRegistry.get(group);
        if (topicSubsriberRegistry == null) {
            topicSubsriberRegistry = new ConcurrentHashMap<String, SubscriberInfo>();
            final ConcurrentHashMap<String/* topic */, SubscriberInfo> old =
                    this.groupTopicSubcriberRegistry.putIfAbsent(group, topicSubsriberRegistry);
            if (old != null) {
                topicSubsriberRegistry = old;
            }
        }
        return topicSubsriberRegistry;
    }


    public MessageListener getMessageListener(final String topic, final String group) throws SimpleMQClientException {
        final ConcurrentHashMap<String, SubscriberInfo> topicSubsriberRegistry =
                this.groupTopicSubcriberRegistry.get(group);
        if (topicSubsriberRegistry == null) {
            return null;
        }
        final SubscriberInfo info = topicSubsriberRegistry.get(topic);
        if (info == null) {
            return null;
        }
        return info.getMessageListener();
    }

    //重新订阅需要移除该组
    public void removeGroup(final String group) {
        this.groupTopicSubcriberRegistry.remove(group);
    }


    ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>> getGroupTopicSubcriberRegistry() {
        return this.groupTopicSubcriberRegistry;
    }
}
