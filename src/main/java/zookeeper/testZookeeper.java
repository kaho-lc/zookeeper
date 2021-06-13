package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author lc
 * @create 2021-05-06-13:46
 */
public class testZookeeper {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private Watcher watcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            List<String> children = null;

            try {
                children = zkClient.getChildren("/", true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (String child : children) {
                System.out.println(child);
            }
        }
    };
    private ZooKeeper zkClient;

    //获取连接
    @Test
    @Before
    public void init() throws IOException {

        zkClient = new ZooKeeper(connectString , sessionTimeout , watcher);
    }

    //1.创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/atguigu", "dahaigezuishuai".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    //2.获取子节点并且监控数据的变化
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    //3.判断节点是否有数据
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/atguigu", false);
        System.out.println(stat==null ? "not exist" : "exist");
    }

}
