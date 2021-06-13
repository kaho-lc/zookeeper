package zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author lc
 * @create 2021-05-06-14:22
 */
public class DistributeServer {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {

        }
    };
    private ZooKeeper zkClient;

    public static void main(String[] args) throws Exception {

        DistributeServer server = new DistributeServer();
        //1.连接到zookeeper集群
        server.getConnect();

        //2.注册节点
        server.regist(args[0]);

        //3.业务逻辑处理
        server.business();
    }

    private void business() throws InterruptedException {

        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {

        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + "is online ");
    }

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString , sessionTimeout , watcher);
    }
}
