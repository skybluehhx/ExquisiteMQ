package com.lin.server.DB;

import com.lin.commons.Message;
import com.lin.server.ConverUtils;
import com.lin.server.DB.constant.DBConstant;
import com.lin.server.DB.model.DBRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author jianglinzou
 * @date 2019/3/14 下午9:30
 */
public class JDBCStoreMessage implements StoreMessage<Message> {


    private final String insertInto = "insert into #{table} (topic,attribute,content,reqId,part,brokerId) " +
            "values(?,?,?,?,?,?)";


    private final String BASE_SELECT_SQL = "select * from #{table} where id > ? and topic = ? limit ?";

    public static Logger logger = LoggerFactory.getLogger(JDBCStoreMessage.class);

    @Override
    public Result<String, Integer> store(Message message) {
        Connection connection = JDBCUtil.getConnection();
        if (Objects.isNull(connection)) {
            logger.error("fail to save message:{} to db because can't get connection", message);
            return Result.fail("the server can't get connection");
        }
        DBRecord dbRecord = ConverUtils.messageTODBRecord(message);
        if (Objects.isNull(dbRecord)) {
            return Result.fail("can't save message because of message is null");
        }
        ResultSet rs = null;
        PreparedStatement preparedStatement = null;
        try {

            String tableName = getCompleteTableName(dbRecord.getPart());
            preparedStatement = connection.prepareStatement(getCompleteSql(tableName), Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setObject(1, dbRecord.getTopic());
            preparedStatement.setObject(2, dbRecord.getAttribute());
            preparedStatement.setObject(3, dbRecord.getContent());
            preparedStatement.setObject(4, dbRecord.getReqId());
            preparedStatement.setObject(5, dbRecord.getPart());
            preparedStatement.setObject(6, dbRecord.getBrokerId());
            preparedStatement.executeUpdate();
            rs = preparedStatement.getGeneratedKeys();
            if (rs.next()) {
                int key = rs.getInt(1);
                return Result.success(key);
            }
            return Result.fail("DB is error,can't get primary key");
        } catch (SQLException e) {
            logger.error("fail to store message because of:{}", e.getMessage());
            return Result.fail(e.getMessage());
        } finally {
            close(rs, preparedStatement, connection);
        }

    }

    @Override
    public Result<String, List<Message>> getMessage(int partition, String topic, int maxSize, long startOffset) {

        List<Message> messages = new ArrayList<>();
        Connection connection = JDBCUtil.getConnection();
        if (Objects.isNull(connection)) {
            logger.error("fail to get message from db because can't get connection");
            return Result.fail("the server can't get connection");
        }
        String tableName = getCompleteTableName(partition);
//        String sql1 = BASE_SELECT_SQL.replace("#{table}", tableName);
//        String sql2 = sql1.replace("#{1}", startOffset + "");
//        String completeSelect = sql2.replace("#{2}", maxSize + "");
        ResultSet rs = null;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(getCompeleteSelectSql(tableName));
            preparedStatement.setObject(1, startOffset);
            preparedStatement.setObject(2, topic);
            preparedStatement.setObject(3, maxSize);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                DBRecord dbRecord = createDBRecord(rs);
                messages.add(ConverUtils.dBRecordTOMessage(dbRecord));
            }
            return Result.success(messages);
        } catch (SQLException e) {
            logger.error("fail to send for consume because of :{}", e);
            return Result.fail("fail to get message because of " + e);
        } finally {
            close(rs, preparedStatement, connection);
        }
    }

    public static void main(String[] args) {
        JDBCStoreMessage jdbcStoreMessage = new JDBCStoreMessage();
        Result<String, List<Message>> list = jdbcStoreMessage.getMessage(1, "lin", 3, 1);
        System.out.println(list.getSuccess());
    }

    private static void close(ResultSet rs, PreparedStatement preparedStatement, Connection connection) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    private DBRecord createDBRecord(ResultSet rs) throws SQLException {
        DBRecord dbRecord = new DBRecord();
        dbRecord.setReqId(rs.getInt("reqId"));
        dbRecord.setId(rs.getInt("id"));
        dbRecord.setBrokerId(rs.getInt("brokerId"));
        dbRecord.setPart(rs.getInt("part"));
        dbRecord.setAttribute(rs.getString("attribute"));
        dbRecord.setContent(rs.getString("content"));
        dbRecord.setTopic(rs.getString("topic"));
        return dbRecord;
    }


    private String getCompleteTableName(int parition) {
        return DBConstant.TablePrefix + parition;
    }

    private String getCompleteSql(String tableName) {
        return insertInto.replace("#{table}", tableName);
    }

    private String getCompeleteSelectSql(String tableName) {
        return BASE_SELECT_SQL.replace("#{table}", tableName);
    }


}
