package exquisiteMQ;

import com.lin.client.MessageSessionFactory;
import com.lin.client.MetaMessageSessionFactory;
import com.lin.client.consumer.ConsumerConfig;
import com.lin.client.consumer.MessageConsumer;
import com.lin.client.consumer.MessageListener;
import com.lin.commons.Message;
import com.lin.commons.exception.SimpleMQClientException;

import java.util.concurrent.Executor;

import static exquisiteMQ.Helper.initMetaConfig;

/**
 * @author jianglinzou
 * @date 2019/3/18 下午5:29
 */
public class TestConsumer {

    public static void main(String[] args) throws SimpleMQClientException {
        // New session factory,强烈建议使用单例
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());

        // subscribed topic
        final String topic = "lin";
        // consumer group
        final String group = "meta-example";
        // create consumer,强烈建议使用单例
        ConsumerConfig consumerConfig = new ConsumerConfig(group);
        // 默认最大获取延迟为5秒，这里设置成100毫秒，请根据实际应用要求做设置。
        consumerConfig.setMaxDelayFetchTimeInMills(100);
        final MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);
        // subscribe topic maxSize,表示一次拉取20条消息
        consumer.subscribe(topic, 20, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                System.out.println("Receive message " + new String(message.getData()));
            }


            @Override
            public Executor getExecutor() {
                // Thread pool to process messages,maybe null.
                return null;
            }
        });
        // complete subscribe
        consumer.completeSubscribe();
    }
}
