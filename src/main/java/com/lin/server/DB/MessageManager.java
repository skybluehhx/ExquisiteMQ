package com.lin.server.DB;

import com.lin.server.DB.dao.DBRecordDao;
import com.lin.server.DB.model.DBRecord;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.Reader;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午9:50
 */
public class MessageManager {

    private static SqlSession sqlSession;

    private static DBRecordDao dbMessageDao;

    static {
        initialConnection();
    }

    public static void main(String[] args) {
        DBRecord dbMessage = new DBRecord();

        dbMessage.setAttribute("11");
        dbMessage.setContent("11");
        dbMessage.setPart(1);
        dbMessage.setReqId(1);
        dbMessage.setTopic("1");
        dbMessage.setBrokerId(1);

        createDBMessage(dbMessage);

    }

    public static void initialConnection() {

        try {

            String resource = "mybatis-config.xml";

            Reader reader = Resources.getResourceAsReader(resource);

            SqlSessionFactory ssf = new SqlSessionFactoryBuilder().build(reader);

            sqlSession = ssf.openSession();

            dbMessageDao = sqlSession.getMapper(DBRecordDao.class);

        } catch (Exception e) {

            e.printStackTrace();

        }

    }


    public static void destroyConnection() {

        sqlSession.close();

    }


    public static void createDBMessage(DBRecord dbMessage) {

        dbMessageDao.createDBMessage(dbMessage);
        System.out.println(dbMessage.getId());
        sqlSession.commit();

    }


}
