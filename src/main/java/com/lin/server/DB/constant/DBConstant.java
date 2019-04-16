package com.lin.server.DB.constant;

/**
 * @author jianglinzou
 * @date 2019/3/14 下午9:28
 */
public interface DBConstant {

    String TablePrefix = "message";

    String createTable = "CREATE TABLE  if not exists  " + "#{table}" +
            " (\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '消息的id',\n" +
            "  `topic` varchar(255) NOT NULL COMMENT '消息的主题',\n" +
            "  `attribute` varchar(255) DEFAULT NULL,\n" +
            "  `content` text COMMENT '消息的内容',\n" +
            "  `reqId` int(11) NOT NULL COMMENT '消息的reqId',\n" +
            "  `part` int(11) NOT NULL,\n" +
            "  `brokerId` int(11) NOT NULL,\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;\n";

}
