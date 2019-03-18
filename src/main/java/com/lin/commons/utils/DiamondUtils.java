package com.lin.commons.utils;

import com.lin.commons.cluster.Partition;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 *
 * 与diamond交互工具类
 * @author jianglinzou
 * @date 2019/3/11 下午1:10
 */
public class DiamondUtils {
    public static final String DEFAULT_ZK_DATAID = "metamorphosis.zkConfig";
    public static final String DEFAULT_PARTITIONS_DATAID = "metamorphosis.partitions";
    static final Log log = LogFactory.getLog(DiamondUtils.class);


    /**
     * 获取zk配置，如果没有抛出运行时NPE
     *
     * @param
     * @param
     * @return
     */
    // public static ZKConfig getZkConfig(final DiamondManager diamondManager,
    // final long timeout) {
    // final Properties props =
    // diamondManager.getAvailablePropertiesConfigureInfomation(timeout);
    // if (props != null) {
    // log.info("从diamond加载zk配置：" + props);
    // return getZkConfig(props);
    // }
    // return null;
    // }

    public static ZkUtils.ZKConfig getZkConfig(final Properties props) {
        if (props != null) {
            boolean zkEnable = true;
            if (!StringUtils.isBlank(props.getProperty("zk.zkEnable"))) {
                zkEnable = Boolean.valueOf(props.getProperty("zk.zkEnable"));
            }
            String zkRoot = "/meta";
            if (!StringUtils.isBlank(props.getProperty("zk.zkRoot"))) {
                zkRoot = props.getProperty("zk.zkRoot");
            }
            final ZkUtils.ZKConfig rt =
                    new ZkUtils.ZKConfig(zkRoot, props.getProperty("zk.zkConnect"), Integer.parseInt(props
                            .getProperty("zk.zkSessionTimeoutMs")), Integer.parseInt(props
                            .getProperty("zk.zkConnectionTimeoutMs")), Integer.parseInt(props
                            .getProperty("zk.zkSyncTimeMs")), zkEnable);

            return rt;
        }
        else {
            throw new NullPointerException("Null zk config");
        }
    }


    // public static void getPartitions(final DiamondManager diamondManager,
    // final long timeout,
    // final Map<String, List<Partition>> partitionsMap) {
    // Properties props = null;
    // try {
    // props =
    // diamondManager.getAvailablePropertiesConfigureInfomation(timeout);
    // }
    // catch (final Exception e) {
    // log.warn(e.getMessage());
    // }
    //
    // log.info("从diamond加载partitions配置：" + props);
    // getPartitions(props, partitionsMap);
    // }

    /** 获取某些topic分区总数的信息(某topic共有哪些分区) */
    public static void getPartitions(final Properties properties, final Map<String, List<Partition>> ret) {
        log.info("开始解析分区总数信息");
        final Map<String, List<Partition>> map = new HashMap<String, List<Partition>>();
        if (properties != null) {
            for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
                final String key = (String) entry.getKey();

                if (key != null && key.startsWith("topic.num.")) {
                    // key取topic
                    final String topic = key.substring("topic.num.".length());
                    // value格式为brokerId0:num0;brokerId1:num1;...
                    final String value = (String) entry.getValue();
                    final List<Partition> partitions = parsePartitions(value);
                    if (partitions != null && !partitions.isEmpty()) {
                        map.put(topic, partitions);
                    }
                }
            }
            ret.clear();
            ret.putAll(map);
            if (!ret.isEmpty()) {
                log.info("分区总数信息: " + map);
            }
            else {
                log.info("empty partitionsNum info");
            }
        }
        else {
            log.warn("Null partitionsNum config");
        }

    }


    private static String trim(final String str, final String stripChars, final int mode) {
        if (str == null) {
            return null;
        }

        final int length = str.length();
        int start = 0;
        int end = length;

        // 扫描字符串头部
        if (mode <= 0) {
            if (stripChars == null) {
                while (start < end && Character.isWhitespace(str.charAt(start))) {
                    start++;
                }
            }
            else if (stripChars.length() == 0) {
                return str;
            }
            else {
                while (start < end && stripChars.indexOf(str.charAt(start)) != -1) {
                    start++;
                }
            }
        }

        // 扫描字符串尾部
        if (mode >= 0) {
            if (stripChars == null) {
                while (start < end && Character.isWhitespace(str.charAt(end - 1))) {
                    end--;
                }
            }
            else if (stripChars.length() == 0) {
                return str;
            }
            else {
                while (start < end && stripChars.indexOf(str.charAt(end - 1)) != -1) {
                    end--;
                }
            }
        }

        if (start > 0 || end < length) {
            return str.substring(start, end);
        }

        return str;
    }


    /**
     * 解析brokerId0:num0;brokerId1:num1;...格式的字符串为partition list
     * */
    static List<Partition> parsePartitions(final String value) {
        final String[] brokerNums = StringUtils.split(value, ";");

        if (brokerNums == null || brokerNums.length == 0) {
            return Collections.emptyList();
        }

        final List<Partition> ret = new LinkedList<Partition>();
        for (String brokerNum : brokerNums) {
            brokerNum = trim(brokerNum, ";", 0);
            final int index = brokerNum.indexOf(":");
            if (index < 0) {
                throw new NumberFormatException("指定的分区个数格式有误,config string=" + value);
            }
            final int brokerId = Integer.parseInt(brokerNum.substring(0, index));
            final int num = Integer.parseInt(brokerNum.substring(index + 1));
            if (num <= 0) {
                throw new NumberFormatException("分区个数必须大于0,config string=" + value);
            }
            for (int i = 0; i < num; i++) {
                ret.add(new Partition(brokerId, i));
            }
        }
        Collections.sort(ret);
        return ret;
    }
}
