package com.lin.commons;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lin.client.MessageUtils;
import com.lin.commons.cluster.Partition;
import com.lin.commons.exception.InvalidMessageException;
import com.lin.commons.utils.JSONUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午5:05
 */
public class MessageIterator {

    private final String topic;
    private final byte[] data;
    private int offset; //表示该消息迭代器的offset
    private Message message;
    private ByteBuffer currentMsgBuf;

    private ListIterator listIterator;


    public MessageIterator(final String topic, final byte[] data) {
        super();
        this.topic = topic;
        this.data = data;
        this.offset = 0;
    }

    public MessageIterator(final String topic, final JSONArray data) {
        super();
        this.topic = topic;
        this.data = new byte[1];
        this.listIterator = data.listIterator();
        this.offset = 0;
    }


    public ByteBuffer getCurrentMsgBuf() {
        return this.currentMsgBuf;
    }


    public int getDataLength() {
        return this.data != null ? this.data.length : 0;
    }


    public void setOffset(final int offset) {
        this.offset = offset;
    }


    public Message getPrevMessage() {
        return this.message;
    }


    /**
     * 返回当前迭代的偏移量，不包括发起请求的偏移量在内
     *
     * @return
     */
    public int getOffset() {
        return this.offset;
    }


    /**
     * 当还有消息的时候返回true
     *
     * @return
     */
    public boolean hasNext() {
        return listIterator.hasNext();
    }


    /**
     * 返回下一个消息
     *
     * @return
     * @throws InvalidMessageException
     */
    public Message next() throws InvalidMessageException {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        String messageString = listIterator.next().toString();
        JSONObject result = JSONUtils.getObject(messageString, JSONObject.class);
        Message message = new Message(result.getString("topic"), result.getString("data"));
        message.setId(result.getLong("id"));
        message.setAttribute(result.getString("attribute"));
        Partition partition = JSONUtils.getObject(result.getString("partition"), Partition.class);
        message.setPartition(partition);
        this.offset++;
        return message;

    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.data);
        result = prime * result + this.offset;
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final MessageIterator other = (MessageIterator) obj;
        if (!Arrays.equals(this.data, other.data)) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.topic == null) {
            if (other.topic != null) {
                return false;
            }
        } else if (!this.topic.equals(other.topic)) {
            return false;
        }
        return true;
    }


    public void remove() {
        throw new UnsupportedOperationException("Unsupported remove");

    }
}
