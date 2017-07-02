import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lebron374 on 2017/7/2.
 */
public class CuratorDemo {

    private static String zkAddr = "127.0.0.1:2181";
    private static String zkPath = "/demo/node";
    private static String zkValue = "hello world";
    private CuratorFramework client = null;

    /**
     * 初始化curator
     */
    public CuratorDemo() {
        client = CuratorFrameworkFactory.newClient(zkAddr, 5000, 5000, new RetryNTimes(10, 1000));
        client.start();
    }

    /**
     * 同步创建节点
     * @throws Exception
     */
    public void createAction() throws Exception {
        String path = client.create().creatingParentsIfNeeded().forPath(zkPath);
        System.out.println("createAction " + path);
    }

    /**
     * 异步创建节点
     * @throws Exception
     */
    public void createActionAsync() throws Exception {
        client.create().creatingParentsIfNeeded().inBackground(new BackgroundCallback() {
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("createAction " + event.toString());
            }
        }).forPath(zkPath);
    }

    /**
     * 同步删除节点
     * @throws Exception
     */
    public void delAction() throws Exception {
        client.delete().deletingChildrenIfNeeded().inBackground(new BackgroundCallback() {
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("delAction " + event.toString());
            }
        }).forPath(zkPath);
    }

    /**
     * 异步删除节点
     * @throws Exception
     */
    public void delActionAsync() throws Exception {
        client.delete().deletingChildrenIfNeeded().inBackground(new BackgroundCallback() {
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                System.out.println("delActionAsync "+ curatorEvent.toString());
            }
        }).forPath("/demo");
    }

    /**
     * 同步设置数据
     * @throws Exception
     */
    public void setAction() throws Exception {
        client.setData().forPath(zkPath, zkValue.getBytes());
    }

    /**
     * 异步设置数据
     * @throws Exception
     */
    public void setActionAsync() throws Exception {
        client.setData().inBackground(new BackgroundCallback() {
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                System.out.println(curatorEvent.getData());
            }
        }).forPath("/demo", zkValue.getBytes());
    }

    /**
     * 获取节点数据
     * @throws Exception
     */
    public void getAction() throws Exception {
        String data = new String(client.getData().forPath(zkPath));
        System.out.println("getAction " + data);
    }

    /**
     * 获取节点列表
     * @throws Exception
     */
    public void getChildrenAction() throws Exception {
        List<String> children = client.getChildren().forPath("/demo");
        for (String child:
             children) {
            System.out.println("getChildrenAction "+child);
        }

    }
    /**
     * Node Cache
     * @throws Exception
     */
    public void nodeCache() throws Exception {
        final NodeCache nodeCache = new NodeCache(client, zkPath);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            public void nodeChanged() throws Exception {
                System.out.println(nodeCache.getCurrentData().getPath());
                System.out.println(new String(nodeCache.getCurrentData().getData()));
            }
        });

        nodeCache.start(true);
    }

    /**
     * Path Cache
     * @throws Exception
     */
    public void pathCache() throws Exception {
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/demo", true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println(new String(pathChildrenCacheEvent.getData().getData()));
                System.out.println(pathChildrenCacheEvent.getData().getPath());
                System.out.println(pathChildrenCacheEvent.getType().toString());
            }
        });

        pathChildrenCache.start();
    }

    /**
     * Tree Cache
     * @throws Exception
     */
    public void treeCache() throws Exception {
        final TreeCache treeCache = new TreeCache(client, "/demo");
        ExecutorService executorService = Executors.newCachedThreadPool();
        treeCache.getListenable().addListener(new TreeCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println(treeCacheEvent.getData().getPath());
                System.out.println(new String(treeCacheEvent.getData().getData()));
                System.out.println(treeCacheEvent.getType());
                System.out.println("------------------------------------------");
            }
        }, executorService);

        treeCache.start();
    }

    /**
     * 创建一次监听事件
     */
    public void createWithWatchOnce() throws Exception {
        client.getCuratorListenable().addListener(new CuratorListener() {
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("监听事件_" + event);
            }
        });

        client.getData().inBackground().forPath(zkPath);
        client.setData().forPath(zkPath, "value".getBytes());
    }

    public static void main(String[] args) throws Exception {
        CuratorDemo demo = new CuratorDemo();

        Thread.sleep(500000);
    }
}
