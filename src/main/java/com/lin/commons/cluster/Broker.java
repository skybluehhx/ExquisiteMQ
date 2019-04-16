package com.lin.commons.cluster;

import com.lin.commons.utils.URIUtils;
import org.apache.commons.lang.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:00
 */
public class Broker {
    private int id; //id全局唯一
    private String host; //broker host
    private int port; //监听的端口号
    private int slaveId = -1; //从


    public Broker(final int id, final String host, final int port) {
        this(id, host, port, -1);
    }


    public Broker(final int id, final String host, final int port, final int slaveId) {
        super();
        this.id = id;
        this.host = host;
        this.port = port;
        this.slaveId = slaveId;
    }


    /**
     * todo 协议名改为 simpleMQ
     * 返回broker节点数据字符串 meta://host:port
     *
     * @return
     */
    public String getZKString() {
        // ipv6
        if (this.host.contains(":")) {
            if (this.host.startsWith("[")) {
                return "exquisitemq://" + this.host + ":" + this.port;
            }
            else {
                return "exquisitemq://[" + this.host + "]:" + this.port;
            }
        }
        else {
            return "exquisitemq://" + this.host + ":" + this.port;
        }
    }


    public int getId() {
        return this.id;
    }


    public void setId(final int id) {
        this.id = id;
    }


    /**
     * @param id
     * @param data
     *            , meta://host:port or meta://host:port?slaveId=xx
     *
     * */
    public Broker(final int id, final String data) {
        this.id = id;
        try {
            final URI uri = new URI(data);
            this.host = uri.getHost();
            this.port = uri.getPort();
            this.slaveId = slaveIdByUri(uri);
        }
        catch (final URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }


    static int slaveIdByUri(final URI uri) {
        final String slaveIdStr = URIUtils.parseParameters(uri, "UTF-8").get("slaveId");
        return StringUtils.isNotBlank(slaveIdStr) ? Integer.parseInt(slaveIdStr) : -1;
    }


    public String getHost() {
        return this.host;
    }


    public void setHost(final String host) {
        this.host = host;
    }


    public int getPort() {
        return this.port;
    }


    public void setPort(final int port) {
        this.port = port;
    }


    public boolean isSlave() {
        return this.slaveId >= 0;
    }


    public int getSlaveId() {
        return this.slaveId;
    }


    @Override
    public String toString() {
        return this.getZKString();
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.host == null ? 0 : this.host.hashCode());
        result = prime * result + this.id;
        result = prime * result + this.port;
        result = prime * result + this.slaveId >= 0 ? this.slaveId : -1;
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
        final Broker other = (Broker) obj;
        if (this.host == null) {
            if (other.host != null) {
                return false;
            }
        }
        else if (!this.host.equals(other.host)) {
            return false;
        }
        if (this.id != other.id) {
            return false;
        }
        if (this.port != other.port) {
            return false;
        }
        if (this.isSlave() != other.isSlave()) {
            return false;
        }
        if (this.isSlave() && other.isSlave()) {
            if (this.slaveId != other.slaveId) {
                return false;
            }
        }
        return true;
    }

}
