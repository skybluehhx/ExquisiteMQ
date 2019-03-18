package exquisiteMQ;

import com.lin.client.MetaClientConfig;
import com.lin.client.MetaMessageSessionFactory;
import com.lin.client.producer.MessageProducer;
import com.lin.client.producer.SendResult;
import com.lin.commons.Message;
import com.lin.commons.utils.ZkUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import static exquisiteMQ.Helper.initMetaConfig;

/**
 * 对生产者的测试
 *
 * @author jianglinzou
 * @date 2019/3/12 下午4:39
 */

public class TestProducer {


    public static void main(String[] args) {

        try {
            MetaMessageSessionFactory metaMessageSessionFactory = new MetaMessageSessionFactory(initMetaConfig());
            final MessageProducer producer = metaMessageSessionFactory.createProducer();
            final String topic = "lin";
            producer.publish(topic);
            while (true) {
                InputStream is = System.in;
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String data = br.readLine();
                final SendResult sendResult = producer.sendMessage(new Message(topic, data));

                if (sendResult.isSuccess()) {
                    System.out.println("Send message successfully,sent to " + sendResult.getOffset());
                } else {
                    System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }




}
