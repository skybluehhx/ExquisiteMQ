package exquisiteMQ;

import com.lin.client.MetaClientConfig;
import com.lin.commons.utils.ZkUtils;

/**
 * @author jianglinzou
 * @date 2019/3/18 下午5:30
 */
public class Helper {

    public static MetaClientConfig initMetaConfig(){

        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
        zkConfig.zkConnect = "127.0.0.1:2181";
        zkConfig.zkRoot = "/meta";
        metaClientConfig.setZkConfig(zkConfig);
        return metaClientConfig;

    }
}
