package org.example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class Ticket12306 implements Runnable {

    private int tickets = 10; // 数据库的票数

    private InterProcessMutex lock;

    public Ticket12306() {
        // 重传策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
        // 1. 第一种方式
//        client = CuratorFrameworkFactory.newClient("192.168.0.104:2181",
//                60 * 1000, 15 * 1000, retryPolicy);

        // 2。 第二种方式
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("192.168.0.104:2181")
                .sessionTimeoutMs(60 * 10000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("itheima")
                .build();
        client.start();
        lock = new InterProcessMutex(client, "/lock");
    }

    @Override
    public void run() {
        while(true) {
            try {
                lock.acquire(3, TimeUnit.SECONDS);
                if (tickets > 0) {
                    System.out.println(Thread.currentThread() + ": " + tickets);
                    tickets--;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    lock.release();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
