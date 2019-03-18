package com.lin.commons.cluster;

import lombok.Data;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表示一个分区
 *
 * @author jianglinzou
 * @date 2019/3/11 下午1:01
 */

@Data
public class Partition implements Comparable<Partition>, Serializable {
    private int brokerId;
    private int partition;
    private String partStr;

    @JsonIgnore
    private transient boolean autoAck = true;
    @JsonIgnore
    private transient boolean acked = false;
    @JsonIgnore
    private transient boolean rollback = false;
    static final long serialVersionUID = -1L;

    public static final Partition RandomPartiton = new Partition(-1, -1);

    private final ConcurrentHashMap<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    private Partition() {

    }

    private Partition(final int brokerId, final int partition, final String partStr) {
        super();
        this.brokerId = brokerId; //服务器id
        this.partition = partition; //分区
        this.partStr = partStr;
    }


    /**
     * 返回是否自动ack，默认为true
     *
     * @return
     */
    @JsonIgnore
    public boolean isAutoAck() {
        return this.autoAck;
    }


    /**
     * 设置是否自动ack
     *
     * @param autoAck true表示自动ack
     */
    public void setAutoAck(final boolean autoAck) {
        this.autoAck = autoAck;
    }


    /**
     * 设置属性，覆盖已有的任何关联值
     *
     * @param key
     * @param value
     */
    public Object setAttribute(final String key, final Object value) {
        return this.attributes.put(key, value);
    }


    /**
     * 当key的value不存在的时候，关联key到传入的value，此操作是原子的
     *
     * @param key
     * @param value
     * @see ConcurrentHashMap#putIfAbsent(Object, Object)
     */
    public Object setAttributeIfAbsent(final String key, final Object value) {
        return this.attributes.putIfAbsent(key, value);
    }


    /**
     * 获取key指定的属性
     *
     * @param key
     * @return
     */
    public Object getAttribute(final String key) {
        return this.attributes.get(key);
    }


    /**
     * 返回属性的key集合，弱一致性
     *
     * @return
     */
    public Set<String> attributeKeySet() {
        return this.attributes.keySet();
    }


    /**
     * 移除key指定的属性
     *
     * @param key
     * @return
     */
    public Object removeAttribute(final String key) {
        return this.attributes.remove(key);
    }


    /**
     * 清空所有属性
     */
    public void clearAttributes() {
        this.attributes.clear();
    }


    /**
     * 返回此partition对象的复制品
     *
     * @return
     */
    public Partition duplicate() {
        return new Partition(this.brokerId, this.partition, this.partStr);
    }


    public Partition(final String str) {
        if (str != null && str.equals(RandomPartiton.toString())) {
            this.partStr = str;
            this.brokerId = RandomPartiton.getBrokerId();
            this.partition = RandomPartiton.getPartition();
        } else {
            if (str == null) {
                throw new IllegalArgumentException("null string");
            }
            final String[] tmps = str.split("-");
            this.partStr = str;
            this.brokerId = Integer.parseInt(tmps[0]);
            this.partition = Integer.parseInt(tmps[1]);
        }
    }


    public Partition(final int brokerId, final int partition) {
        super();
        this.brokerId = brokerId;
        this.partition = partition;
        this.partStr = brokerId + "-" + partition;
    }


    /**
     * 返回broker id
     *
     * @return
     */
    public int getBrokerId() {
        return this.brokerId;
    }


    /**
     * 返回分区id
     *
     * @return
     */
    public int getPartition() {
        return this.partition;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.brokerId;
        result = prime * result + this.partition;
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
        final Partition other = (Partition) obj;
        if (this.brokerId != other.brokerId) {
            return false;
        }
        if (this.partition != other.partition) {
            return false;
        }
        return true;
    }


    public int compareTo(final Partition o) {
        if (this.brokerId != o.brokerId) {
            return this.brokerId > o.brokerId ? 1 : -1;
        } else if (this.partition != o.partition) {
            return this.partition > o.partition ? 1 : -1;
        }
        return 0;
    }


    /**
     * 应答本分区自上次应答以来收到的消息，meta客户端将递增offset，仅在设置autoAck为false的时候有效
     */
    public void ack() {
        if (this.isAutoAck()) {
            throw new IllegalStateException("Partition is in auto ack mode");
        }
        if (this.isRollback()) {
            throw new IllegalStateException("Could not ack rollbacked partition");
        }
        this.acked = true;
    }


    /**
     * 返回本分区是否调用了ack，如果是自动ack模式，则永远返回true
     *
     * @return
     */
    @JsonIgnore
    public boolean isAcked() {
        return this.isAutoAck() || this.acked;
    }


    /**
     * 返回本分区是否调用了rollback，如果是自动ack模式则永远返回false
     *
     * @return
     */
    @JsonIgnore
    public boolean isRollback() {
        return !this.isAutoAck() && this.rollback;
    }


    /**
     * 回滚自上次应答以来收到的消息,meta将重试投递这些消息,仅在设置autoAck为false的时候有效
     */
    public void rollback() {
        if (this.isAutoAck()) {
            throw new IllegalStateException("Partition is  in auto ack mode");
        }
        if (this.isAcked()) {
            throw new IllegalStateException("Could not rollback acked partition");
        }
        this.rollback = true;
    }


    public void reset() {
        this.rollback = false;
        this.acked = false;
    }


    @Override
    public String toString() {
        return this.partStr;
    }
}
