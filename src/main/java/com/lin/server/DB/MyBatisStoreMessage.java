package com.lin.server.DB;

import com.lin.commons.Message;
import com.lin.server.ConverUtils;
import com.lin.server.DB.model.DBRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * 基于mybatis的保存消息的接口，后续考虑使用原生jdbc
 *
 * @author jianglinzou
 * @date 2019/3/12 下午3:32
 */
public class MyBatisStoreMessage implements StoreMessage<Message> {

    public static Logger logger = LoggerFactory.getLogger(MyBatisStoreMessage.class);

    @Override
    public Result<String, Integer> store(Message Message) {
        DBRecord dbRecord = ConverUtils.messageTODBRecord(Message);
        if (Objects.isNull(dbRecord)) {
            logger.warn("the message is null,so you don't save it");
            return Result.fail("the message is null,so you don't save it");
        }
        try {
            MessageManager.createDBMessage(dbRecord);
        } catch (Throwable e) {
            logger.error("fail to save to message because of :{}", e);
            return Result.fail("fail to save to message because of " + e.getMessage());
        }

        return Result.success(dbRecord.getId());

    }

    @Override
    public Result<String, List<Message>> getMessage(int partition, String topic, int maxSize, long startOffset) {
        return null;
    }
}
