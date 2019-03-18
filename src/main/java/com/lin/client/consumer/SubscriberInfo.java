package com.lin.client.consumer;

import com.lin.commons.filter.ConsumerMessageFilter;

/**
 * 订阅者信息
 * @author jianglinzou
 * @date 2019/3/11 下午1:25
 */
public class SubscriberInfo {

    private final MessageListener messageListener;
    private final ConsumerMessageFilter consumerMessageFilter;
    private final int maxSize;


    public SubscriberInfo(final MessageListener messageListener, final ConsumerMessageFilter consumerMessageFilter,
                          final int maxSize) {
        super();
        this.messageListener = messageListener;
        this.maxSize = maxSize;
        this.consumerMessageFilter = consumerMessageFilter;
    }


    public ConsumerMessageFilter getConsumerMessageFilter() {
        return this.consumerMessageFilter;
    }


    public MessageListener getMessageListener() {
        return this.messageListener;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.consumerMessageFilter == null ? 0 : this.consumerMessageFilter.hashCode());
        result = prime * result + this.maxSize;
        result = prime * result + (this.messageListener == null ? 0 : this.messageListener.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        SubscriberInfo other = (SubscriberInfo) obj;
        if (this.consumerMessageFilter == null) {
            if (other.consumerMessageFilter != null) {
                return false;
            }
        }
        else if (!this.consumerMessageFilter.equals(other.consumerMessageFilter)) {
            return false;
        }
        if (this.maxSize != other.maxSize) {
            return false;
        }
        if (this.messageListener == null) {
            if (other.messageListener != null) {
                return false;
            }
        }
        else if (!this.messageListener.equals(other.messageListener)) {
            return false;
        }
        return true;
    }


    public int getMaxSize() {
        return this.maxSize;
    }
}
