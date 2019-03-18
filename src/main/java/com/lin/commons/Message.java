package com.lin.commons;

import com.lin.commons.cluster.Partition;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 标识具体的一条消息
 *
 * @author jianglinzou
 * @date 2019/3/8 下午6:07
 */
@Data
public class Message implements Serializable {
    static final long serialVersionUID = -1L;
    private String Idgenetate; //有id 生成，全局唯一
    private long id; //消息id
    private String topic;//消息所在的topic
    private String data;//消息的内容
    private String attribute;//消息的属性
    private int flag;//如果attributa不为空，该标志位为1
    private Partition partition; //消息所在topic下的哪个分区

    private transient boolean readOnly; //是否只读

    private transient boolean rollbackOnly = false;

    public Message() {

    }

    public Message(final String topic, final String data) {
        super();
        this.topic = topic;
        this.data = data;
    }


    public Message(final String topic, final String data, final String attribute) {
        super();
        this.topic = topic;
        this.data = data;
        this.attribute = attribute;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.attribute == null ? 0 : this.attribute.hashCode());
        result = prime * result + Arrays.hashCode(this.data.getBytes());
        result = prime * result + (int) (this.id ^ this.id >>> 32);
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
        final Message other = (Message) obj;
        if (this.attribute == null) {
            if (other.attribute != null) {
                return false;
            }
        } else if (!this.attribute.equals(other.attribute)) {
            return false;
        }
        if (!Arrays.equals(this.data.getBytes(), other.data.getBytes())) {
            return false;
        }
        if (this.id != other.id) {
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

}