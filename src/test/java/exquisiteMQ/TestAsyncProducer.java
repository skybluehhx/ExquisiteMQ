package exquisiteMQ;

import com.lin.client.MessageSessionFactory;
import com.lin.client.MetaMessageSessionFactory;
import com.lin.client.producer.MessageProducer;
import com.lin.client.producer.SendMessageCallback;
import com.lin.client.producer.SendResult;
import com.lin.commons.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static exquisiteMQ.Helper.initMetaConfig;

/**
 * 异步消息发送者
 *
 * @author jianglinzou
 * @date 2019/3/18 下午5:40
 */
public class TestAsyncProducer {

    public static void main(final String[] args) throws Exception {
        // New session factory,强烈建议使用单例
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());
        // create producer,强烈建议使用单例
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = "lin";
        producer.publish(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while ((line = readLine(reader)) != null) {
            // send message
            try {
                producer.sendMessage(new Message(topic, line), new SendMessageCallback() {

                    @Override
                    public void onMessageSent(final SendResult result) {
                        if (result.isSuccess()) {
                            System.out.println("Send message successfully,sent to " + result.getPartition());

                        } else {
                            System.err.println("Send message failed,error message:" + result.getErrorMessage());
                        }

                    }


                    @Override
                    public void onException(final Throwable e) {
                        e.printStackTrace();

                    }
                });

            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type a message to send:");
        return reader.readLine();
    }
}
