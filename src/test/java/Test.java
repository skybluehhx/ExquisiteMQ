import com.lin.commons.cluster.Partition;
import com.lin.commons.utils.JSONUtils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:29
 */
public class Test {
    public static void main(String[] args) {
        ConcurrentHashMap map = new ConcurrentHashMap();
        map.put("1", "hello");
        System.out.println(map.putIfAbsent("1", "1234"));
        System.out.println(map.get("1"));
        testPartition();
    }

    public static void testPartition() {

        Partition partition = new Partition("0-1");
        String partString = JSONUtils.toJsonString(partition);
        System.out.println(partString);
        Partition partition1 = JSONUtils.getObject(partString, Partition.class);
        int brokerId = partition1.getBrokerId();
        int partId = partition1.getPartition();
        System.out.println(brokerId);
        System.out.println(partId);

    }
}
