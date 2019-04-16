package com.lin.server;

import com.lin.commons.cluster.json.TopicBroker;
import com.lin.commons.utils.JSONUtils;
import com.lin.commons.utils.PropUtils;
import com.lin.commons.utils.ZkUtils;
import com.lin.commons.utils.zk.MetaZookeeper;
import com.lin.server.DB.JDBCUtil;
import com.lin.server.DB.constant.DBConstant;
import com.lin.server.Exception.ExquisiteMQException;
import com.lin.server.Exception.FailLoadConfFileException;
import com.lin.server.config.BrokerConfig;
import com.lin.server.config.SalveConfig;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * 默认的ExquisiteMQ服务器
 *
 * @author jianglinzou
 * @date 2019/3/14 下午2:41
 */
public class DefaultExquisiteMQServer implements ExquisiteMQServer {
    public static Logger logger = LoggerFactory.getLogger(DefaultExquisiteMQServer.class);

    DefaultNettyServer defaultNettyServer = new DefaultNettyServer();

    private ZkUtils.ZKConfig zkConfig;

    private BrokerConfig brokerConfig;

    private ZkClient zkClient;

    private MetaZookeeper metaZookeeper;

    public void start() throws Exception {
        logger.info("start to start ExquisiteMQServer");
        init();
        registerInfoToZk();
        startRemoteServer();//开启远程服务
    }

    @Override
    public void stop() {
        defaultNettyServer.close();
        zkClient.close();
    }


    private void init() throws ExquisiteMQException {
        initConfig();
        initTables();
    }

    private void initTables() throws ExquisiteMQException {

        int part = brokerConfig.getNumPartitions();
        for (int i = 0; i < part; i++) {
            try {
                JDBCUtil.createPartitionTableIfNotExist(DBConstant.TablePrefix + i);
            } catch (SQLException e) {
                logger.error("fail to create table:{} because of :{}", e);
                throw new ExquisiteMQException("fail to create table for partition " + i);
            }

        }


    }

    private void initConfig() throws FailLoadConfFileException {

        try {
            final Properties properties = PropUtils.getResourceAsProperties("serverini.properties", "UTF-8");
            zkConfig = getZKConfigFromProp(properties);
            brokerConfig = getBrokerConfigFromProp(properties);
            checkConfig();
        } catch (Exception e) {
            logger.error("fail to start server because of :{}", e.getMessage());
            throw new FailLoadConfFileException(e);
        }


    }

    /**
     * 注册 broker中现有的信息到zk上
     */
    private void registerInfoToZk() throws Exception {
        logger.info("start to register information to zk");
        if (this.zkConfig == null) {
            throw new ExquisiteMQException("No zk conf offered");
        }
        if (this.zkConfig != null) {
            this.zkClient = new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
            this.metaZookeeper = new MetaZookeeper(zkClient, this.zkConfig.zkRoot);
        }
        registerBrokerIds();
        for (String defaultTopic : brokerConfig.getDefaultTopics()) {
            registerDefaultTopic(defaultTopic);
        }


    }

    /**
     * 注册topic信息到topic上
     */
    private void registerDefaultTopic(String topic) throws Exception {
        logger.info("start to register deault topic's informaion to zk");
        String pubTopicPath = metaZookeeper.brokerTopicsPubPath + "/" + topic;
        String subTopicPath = metaZookeeper.brokerTopicsSubPath + "/" + topic;
        ZkUtils.makeSurePersistentPathExists(zkClient, pubTopicPath);
        ZkUtils.makeSurePersistentPathExists(zkClient, subTopicPath);
        String pubTopicBroker = pubTopicPath + "/" + brokerConfig.getBrokerId() + "-m";
        String subTopicBroker = subTopicPath + "/" + brokerConfig.getBrokerId() + "-m";
        TopicBroker topicBroker = new TopicBroker();
        topicBroker.setBroker(brokerConfig.getBrokerId() + "-m");
        topicBroker.setNumParts(brokerConfig.getNumPartitions());
        String jsonTopicBroker = JSONUtils.toJsonString(topicBroker);
        ZkUtils.createEphemeralPath(zkClient, pubTopicBroker, jsonTopicBroker); //注册在该topic下的broker
        ZkUtils.createEphemeralPath(zkClient, subTopicBroker, jsonTopicBroker); //注册在该topic下的broker

    }

    /**
     * 在/meta/brokers/ids 注册该broker的信息
     *
     * @throws Exception
     */
    private void registerBrokerIds() throws Exception {
        logger.info("register broker's information to zk");
        ZkUtils.makeSurePersistentPathExists(zkClient, metaZookeeper.brokerIdsPath);
        String borkerPath = metaZookeeper.brokerIdsPath + "/" + brokerConfig.getBrokerId();
        StringBuilder stringBuilder = new StringBuilder(zkConfig.getZkRoot().substring(1)).append(":").append("//");
        String brokersData = stringBuilder.append(brokerConfig.getHostName()).append(":").append(brokerConfig.getServerPort()).toString();
        ZkUtils.makeSurePersistentPathExists(zkClient, borkerPath);
//        ZkUtils.createEphemeralPath(zkClient, borkerPath, null);
        String masterPath = borkerPath + "/master";
        ZkUtils.createEphemeralPath(zkClient, masterPath, brokersData);
        registerSalves(borkerPath);
    }

    private void registerSalves(String borkerPath) throws Exception {
        logger.info("start to register salves to zk");
        for (int i = 0; i < brokerConfig.getSalves().size(); i++) {
            String salvePath = borkerPath + "/salve" + i;
            StringBuilder stringBuilder = new StringBuilder(zkConfig.getZkRoot()).append(":").append("//");
            String salveData = stringBuilder.append(brokerConfig.getSalves().get(i).getSalveHost()).append(":").append(brokerConfig.getSalves().get(i).getPort()).toString();
            ZkUtils.createEphemeralPath(zkClient, salvePath, salveData);

        }
    }


    /**
     * 启动远程服务器，即netty服务器
     */
    public void startRemoteServer() throws Exception {
        logger.info("start netty server,the prot:{}", brokerConfig.getServerPort());
        defaultNettyServer.bind(brokerConfig.getServerPort());
    }


    private void checkConfig() throws FailLoadConfFileException {
        if (Objects.isNull(zkConfig) || Objects.isNull(brokerConfig)) {
            throw new FailLoadConfFileException("can't occur,please retry again");
        }
        if (StringUtils.isBlank(zkConfig.zkConnect)) {
            throw new FailLoadConfFileException("please set zkConnect ");
        }
        if (StringUtils.isBlank(brokerConfig.getHostName())) {
            throw new FailLoadConfFileException("please set hostName");
        }
        if (Objects.isNull(brokerConfig.getBrokerId())) {
            throw new FailLoadConfFileException("please set brokerId");
        }
        if (Objects.isNull(brokerConfig.getNumPartitions())) {
            throw new FailLoadConfFileException("please set numPartitions");
        }
        if (Objects.isNull(brokerConfig.getServerPort())) {
            throw new FailLoadConfFileException("please set serverPort");
        }
        return;

    }

    private ZkUtils.ZKConfig getZKConfigFromProp(Properties properties) {
        logger.info("start to load zkConfig");
        final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
        if (StringUtils.isNotBlank(properties.getProperty("zk.zkConnect"))) {
            zkConfig.zkConnect = properties.getProperty("zk.zkConnect");
        }
        if (StringUtils.isNotBlank(properties.getProperty("zk.zkSessionTimeoutMs"))) {
            zkConfig.zkSessionTimeoutMs = Integer.parseInt(properties.getProperty("zk.zkSessionTimeoutMs"));
        }
        if (StringUtils.isNotBlank(properties.getProperty("zk.zkConnectionTimeoutMs"))) {
            zkConfig.zkConnectionTimeoutMs = Integer.parseInt(properties.getProperty("zk.zkConnectionTimeoutMs"));
        }
        if (StringUtils.isNotBlank(properties.getProperty("zk.zkSyncTimeMs"))) {
            zkConfig.zkSyncTimeMs = Integer.parseInt(properties.getProperty("zk.zkSyncTimeMs"));
        }
        return zkConfig;
    }

    private BrokerConfig getBrokerConfigFromProp(Properties properties) {
        logger.info("start to load brokerConfig");
        final BrokerConfig brokerConf = new BrokerConfig();
        if (StringUtils.isNotBlank(properties.getProperty("brokerId"))) {
            brokerConf.setBrokerId(Integer.parseInt(properties.getProperty("brokerId").trim()));
        }
        if (StringUtils.isNotBlank(properties.getProperty("hostName"))) {
            brokerConf.setHostName(properties.getProperty("hostName").trim());
        }
        if (StringUtils.isNotBlank(properties.getProperty("numPartitions"))) {
            brokerConf.setNumPartitions(Integer.parseInt(properties.getProperty("numPartitions").trim()));
        }
        if (StringUtils.isNotBlank(properties.getProperty("serverPort"))) {
            brokerConf.setServerPort(Integer.parseInt(properties.getProperty("serverPort").trim()));
        }
        if (StringUtils.isNotBlank(properties.getProperty("salve"))) {
            String salvesConfigString = properties.getProperty("salve");
            String[] salvelist = salvesConfigString.split(","); //多个以逗号分隔
            if (Objects.isNull(salvelist) || salvelist.length <= 0) {
            } else {
                for (String salveString : salvelist) {
                    String[] salves = salveString.split(":"); //地址与端口号间以":"分隔
                    SalveConfig salveConfig = new SalveConfig();
                    salveConfig.setSalveHost(salves[0].trim());
                    salveConfig.setPort(Integer.parseInt(salves[1].trim()));
                    brokerConf.addSalveConfig(salveConfig);
                }
            }

        }
        if (StringUtils.isNotBlank(properties.getProperty("defaultTopic"))) {
            String defaultTopics = properties.getProperty("defaultTopic");
            String[] topics = defaultTopics.split(",");
            if (Objects.nonNull(topics) && topics.length > 0) {
                for (String topic : topics) {
                    brokerConf.getDefaultTopics().add(topic);
                }
            }
        }
        return brokerConf;
    }

    public static void main(String[] args) throws Exception {
        DefaultExquisiteMQServer defaultExquisiteMQServer = new DefaultExquisiteMQServer();
        defaultExquisiteMQServer.start();
        System.out.println("salve1".substring(6));
    }

}
