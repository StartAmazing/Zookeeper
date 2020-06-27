package com.ll.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Liuliang
 * Create by Liuliang on 2020/06/27
 */
public class ZkClient {

    ZooKeeper zooKeeper;

    @Before
    public void init() throws IOException {

        String conn = "192.168.222.11:12181";

        zooKeeper = new ZooKeeper(conn, 6000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getState());
                System.out.println(watchedEvent.getType());
                System.out.println(watchedEvent.getWrapper());
                System.out.println(watchedEvent);
            }
        });

    }

    /**
     * 获取节点数据
     */
    @Test
    public void getData() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/tuling", false, null);
        System.out.println(new String(data));
    }

    /**
     * 获取节点数据
     */
    @Test
    public void getData2() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/tuling", true, null);
        System.out.println(new String(data));
        Thread.sleep(30000);
    }

    /**
     * 获取节点数据，自定义监听
     */
    @Test
    public void getData3() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        zooKeeper.getData("/tuling", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    zooKeeper.getData(event.getPath(), this, null);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(event.getPath());
            }
        }, stat);
        System.out.println(stat);
        Thread.sleep(30000);
    }

    /**
     * 获取节点数据，getCallBack
     */
    @Test
    public void getData4() throws KeeperException, InterruptedException {
        zooKeeper.getData("/tuling", false, new AsyncCallback.DataCallback() {

            // 当有返回结果的时候会触发以下方法，使得拿到的数据更加全面

            /**
             * @param rc 获取节点是否成功的相关错误码
             * @param path 传入的path
             * @param ctx getData的下一个参数原封不动传入这里
             * @param data 获得的值
             * @param stat 相关属性
             */
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println(stat);
            }
        }, "");
        Thread.sleep(30000);
    }

    /**
     * 检查子节点
     */
    @Test
    public void getChild() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/tuling", false);
        children.stream().forEach(System.out::println);
    }

    /**
     * 检查子节点
     */
    @Test
    public void getChild2() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/tuling", event -> {
            // 子节点发生变更的时候打印的是父节点路径
            System.out.println(event.getPath());
        });
        children.stream().forEach(System.out::println);
        Thread.sleep(30000);
    }

    /**
     * 检查子节点
     */
    @Test
    public void getChild3() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/tuling", event -> {
            System.out.println(event.getPath());
        });
        children.stream().forEach(System.out::println);
        Thread.sleep(30000);
    }


    /**
     * 创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        // 数字位移
//        @InterfaceAudience.Public
//        public interface Perms {
//            int READ = 1 << 0;
//
//            int WRITE = 1 << 1;
//
//            int CREATE = 1 << 2;
//
//            int DELETE = 1 << 3;
//
//            int ADMIN = 1 << 4;
//
//            int ALL = READ | WRITE | CREATE | DELETE | ADMIN;
//        }
//        int perm = 31; // cdwra
        // admin的权限是指能不能给该节设置ACL
        int perm = ZooDefs.Perms.ADMIN | ZooDefs.Perms.CREATE |
                ZooDefs.Perms.DELETE | ZooDefs.Perms.READ | ZooDefs.Perms.WRITE;
        ACL acl1 = new ACL(perm, new Id("world", "anyone"));
        ACL acl2 = new ACL(perm, new Id("ip", "192.168.222.11"));
        ACL acl3 = new ACL(perm, new Id("ip", "127.0.0.1"));
        List<ACL> aclList = new ArrayList<>();
        aclList.add(acl1);
        aclList.add(acl2);
        aclList.add(acl3);
        zooKeeper.create("/tuling/can", "hello, zookeeper".getBytes(), aclList, CreateMode.PERSISTENT);
    }

}
