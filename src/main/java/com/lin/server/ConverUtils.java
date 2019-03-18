package com.lin.server;

import com.lin.commons.Message;
import com.lin.commons.cluster.Partition;
import com.lin.commons.utils.JSONUtils;
import com.lin.server.DB.model.DBRecord;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午10:35
 */
@Data
public class ConverUtils {

    private DBRecord dbRecord;

    public static Logger logger = LoggerFactory.getLogger(ConverUtils.class);

    public static DBRecord messageTODBRecord(Message message) {
        if (Objects.isNull(message)) {
            return null;
        }
        System.out.println((message.getTopic() + ":" + message.getData() + ":" + message.getAttribute() + ":" + message.getPartition()));
        DBRecord dbRecord = new DBRecord();
        dbRecord.setTopic(message.getTopic());
        dbRecord.setContent(message.getData());
        dbRecord.setAttribute(message.getAttribute());
        dbRecord.setPart(message.getPartition().getPartition());
        dbRecord.setBrokerId(message.getPartition().getBrokerId());
        return dbRecord;
    }


    public static Message dBRecordTOMessage(DBRecord dbRecord) {

        Message message = new Message();
        message.setId(dbRecord.getId());
        message.setAttribute(dbRecord.getAttribute());
        message.setData(dbRecord.getContent());
        message.setTopic(dbRecord.getTopic());
        Partition partition = new Partition(dbRecord.getBrokerId() + "-" + dbRecord.getPart());
//        partition.setPartition(dbRecord.getPart());
//        partition.setPartition(dbRecord.getBrokerId());
        message.setPartition(partition);
        return message;

    }

    public static void main(String[] args) {
//        String json = "{\"id\":0,\"topic\":\"meta-test\",\"data\":\"123\",\"attribute\":null,\"flag\":0,\"partition\":{\"brokerId\":0,\"part\":0,\"autoAck\":true,\"acked\":true,\"rollback\":false},\"readOnly\":false,\"rollbackOnly\":false,\"idgenetate\":null}";
//        Message message = JSONUtils.getObject(json, Message.class);
//
//        JSONObject jsonObject = JSONUtils.getJSONObject("{\"id\":0,\"topic\":\"meta-test\",\"data\":\"123\",\"attribute\":null,\"flag\":0,\"partition\":{\"brokerId\":0,\"autoAck\":true,\"acked\":true,\"rollback\":false},\"readOnly\":false,\"rollbackOnly\":false,\"idgenetate\":null}");
//        System.out.println(1);
////        Message message1 = JSONObject.getON(json, Message.class);
//        JSONObject t = jsonObject;
//        System.out.println(message.getPartition());

        Message converUtils = new Message();
        Partition partition = new Partition("0-1");
        partition.setPartition(1);
        converUtils.setPartition(partition);

        String str = JSONUtils.toJsonString(converUtils);
        System.out.println(str);
        Message converUtils1 = JSONUtils.getObject(str, Message.class);
        System.out.println(converUtils1.getPartition());

    }


}


