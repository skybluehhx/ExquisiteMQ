package com.lin.server.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 保存broker的配置文件，其中存有brokerId,broker的地址以及分区数，监听的端口号等先关信息
 *
 * @author jianglinzou
 * @date 2019/3/14 下午2:55
 */
@Data
public class BrokerConfig {

    private Integer brokerId;

    private String hostName;

    private Integer numPartitions;

    private Integer serverPort;

    private List<String> defaultTopics = new ArrayList<>();


    private List<SalveConfig> salves = new ArrayList<>();


    public void addSalveConfig(SalveConfig salveConfig) {
        this.salves.add(salveConfig);
    }

}
