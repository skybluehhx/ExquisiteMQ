package com.lin.server.DB;

/**
 * Created by zoujianglin
 * 2018/8/19 0019.
 */


import com.lin.commons.utils.PropUtils;
import com.lin.server.DB.constant.DBConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;


public class JDBCUtil {


    public static Logger logger = LoggerFactory.getLogger(JDBCUtil.class);
    //url
    private static String url = null;
    //user
    private static String username = null;
    //password
    private static String password = null;
    //驱动程序类
    private static String driver = null;

    /**
     * 只注册一次，静态代码块
     */
    static {

        //注册驱动程序
        try {

            /**
             * 读取jdbc.properties文件
             */
            final Properties prop = PropUtils.getResourceAsProperties("jdbc.properties", "UTF-8");
            url = prop.getProperty("url");
            username = prop.getProperty("username");
            password = prop.getProperty("password");
            driver = prop.getProperty("driver");
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取连接方法
     */
    public static Connection getConnection() {
        try {
            Connection conn = DriverManager.getConnection(url, username, password);
            return conn;
        } catch (SQLException e) {
            logger.info("fail to get connect ,the url:{},user:{},because of:{}", url, username, e.getMessage());
            return null;
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(Statement stmt, Connection conn) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.info("fail to close statement:{} because of :{}", stmt, e.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.info("fail to close connection because of:{}", e.getMessage());
            }
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.info("fail to close re:{} because of :{}", rs, e.getMessage());
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.info("fail to close statement:{} because of :{}", stmt, e.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.info("fail to close connection:{} because of :{}", conn, e.getMessage());
            }
        }
    }


    public static boolean createPartitionTableIfNotExist(String tableName) throws SQLException {
        String createTableSql = DBConstant.createTable.replace("#{table}", tableName);
        PreparedStatement preparedStatement = getConnection().prepareStatement(createTableSql);
        int result = preparedStatement.executeUpdate();
        if (result == 0) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {

    }


}

