package org.example;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CuratorTest {

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

    /*************************************************************************************/

    @Test
    public void testCreate1() throws Exception {
        // 1. 基本节点创建
        // 如果创建节点，没有指定数据，则默认将当前客户端的 IP 作为数据存储
        String path = client.create().forPath("/app1");
        System.out.println(path);
    }

    @Test
    public void testCreate2() throws Exception {
        // 2. 基本节点创建
        // 创建节点时，指定数据存储
        String path = client.create().forPath("/app2", "hehe".getBytes());
        System.out.println(path);
    }

    @Test
    public void testCreate3() throws Exception {
        // 3. 设置节点类型
        // 默认类型：持久化
        String path = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
        System.out.println(path);
        while(true) {

        }
    }

    @Test
    public void testCreate4() throws Exception {
        // 4. 创建多级节点
        // creatingParentsIfNeeded(): 如果父节点不存在，则创建父节点
        String path = client.create().creatingParentsIfNeeded().forPath("/app4/p1");
        System.out.println(path);
    }

    /*************************************************************************************/

    /**
     * 查询节点
     * 1. 查询节点：get
     * 2. 查询子节点：ls
     * 3. 查询节点状态信息：ls -s
     * */

    @Test
    public void testGet1() throws Exception {
        byte[] data = client.getData().forPath("/app1");
        System.out.println(new String(data));
    }

    @Test
    public void testGet2() throws Exception {
        List<String> path = client.getChildren().forPath("/app4");
        System.out.println(path);
    }

    @Test
    public void testGet3() throws  Exception {
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath("/app1");
        System.out.println(stat);
    }

    /*************************************************************************************/

    /**
     * 修改数据
     * 1. 修改数据
     * 2. 根据版本修改
     * @throws  Exception
     * */
    @Test
    public void testSet1() throws Exception {
        client.setData().forPath("/app1","itcast".getBytes());
    }

    @Test
    public void testSet2() throws Exception {
        // 只有当 version 前后一致，才能修改，否则报错
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath("/app1");
        int version = stat.getVersion();
        System.out.println(version);
        client.setData().withVersion(version).forPath("/app1","haha".getBytes());
//        client.setData().withVersion(100).forPath("/app1","haha".getBytes());
    }

    /*************************************************************************************/

    /**
     * 删除数据
     * 1. 删除单个节点
     * 2. 删除带有子节点的节点
     * 3. 必须成功的删除: 为了防止网络抖动。本质就是重试。
     * 4. 回调
     * @throws  Exception
     * */

    @Test
    public void testDelete1() throws Exception {
        // 1. 删除单个节点
        client.delete().forPath("/app1");
    }

    @Test
    public void testDelete2() throws Exception {
        // 2. 删除带有子节点的节点
        client.delete().deletingChildrenIfNeeded().forPath("/app4");
    }

    @Test
    public void testDelete3() throws Exception {
        // 3. 必须成功的删除
        client.delete().guaranteed().forPath("/app4");
    }

    @Test
    public void testDelete4() throws  Exception {
        // 4. 回调
        client.delete().guaranteed().inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                System.out.println("我被删除了");
                System.out.println(curatorEvent);
            }
        }).forPath("/app2");
    }

    @After
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
