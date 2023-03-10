package org.example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CuratorWatchTest {

    private CuratorFramework client;

    @Before
    public void init() {

        /**
         * @param connectString        连接字符串，zk Server地址和端口 "127.0.0.1:2181"
         * @param sessionTimeoutMs     session timeout
         * @param connectionTimeOutMs  connection timeout
         * @param retryPolicy          retry policy to use
         * */
        // 重传策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
        // 1. 第一种方式
//        client = CuratorFrameworkFactory.newClient("192.168.0.104:2181",
//                60 * 1000, 15 * 1000, retryPolicy);

        // 2。 第二种方式
        client = CuratorFrameworkFactory.builder()
                .connectString("192.168.0.104:2181")
                .sessionTimeoutMs(60 * 10000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("itheima")
                .build();
        client.start();
    }

    /**
     * 演示 NodeCache：给指定一个节点注册监听器
     * */
    @Test
    public void testNodeCache() throws Exception {
        // 1. 创建 NodeCache 对象
        NodeCache nodeCache = new NodeCache(client, "/app1");

        // 2. 注册监听
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点变化了～");
                // 获取节点修改后的数据
                byte[] bytes = nodeCache.getCurrentData().getData();
                System.out.println(new String(bytes));
            }
        });

        // 3. 开启监听. 如果设置为 true, 则开启监听时，加载缓冲数据
        nodeCache.start(true);
        while(true) {

        }
    }

    @Test
    public void testPathChildrenCache() throws Exception {
        // 1. 创建监听对象
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app2", true);
        // 2. 绑定监听器
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("子节点变化了～");
                System.out.println(pathChildrenCacheEvent);
            }
        });
        // 3. 开启
        pathChildrenCache.start();
        while(true) {

        }
    }

    @Test
    public void testTreeCache() throws Exception {
        // 1. 创建监听器
        TreeCache treeCache = new TreeCache(client, "/app2");
        // 2. 注册监听
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("节点/孩子发型变化～");
                System.out.println(treeCacheEvent);
            }
        });
        // 3. 开启监听
        treeCache.start();
    }

    @After
    public void close() {
        if (client != null) {
            client.close();
        }
    }

}
