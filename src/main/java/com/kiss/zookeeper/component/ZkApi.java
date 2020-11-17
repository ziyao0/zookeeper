package com.kiss.zookeeper.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

/**
 * @author zhangziyao
 */
@Component
@Slf4j
public class ZkApi {


    @Autowired
    private WatcherApi watcherApi;

    @Resource
    private ZooKeeper zooKeeper;

    /**
     * 判断指定节点是否存在
     *
     * @param path      节点路径
     * @param needWatch 指定是否复用zookeeper中默认{@link Watcher}
     * @return 返回 {@link Stat}
     */
    public Stat exists(String path, boolean needWatch) {
        try {
            return zooKeeper.exists(path, needWatch);
        } catch (Exception e) {
            log.error("判断指定节点是否存在异常:{},{}", path, e);
            return null;
        }
    }

    /**
     * 检测节点是否存在 并设置监听事件
     *
     * @param path    节点路径
     * @param watcher 传入指定的监听类 {@link Watcher}
     * @return 返回 {@link Stat}
     */
    public Stat exists(String path, Watcher watcher) {
        try {
            return zooKeeper.exists(path, watcher);
        } catch (Exception e) {
            log.error("判断指定节点是否存在异常:{},{}", path, e);
            return null;
        }
    }

    /**
     * 创建持久化节点
     *
     * @param path 节点路径
     * @param data 节点信息
     * @return 返回是否穿件成功
     */
    public boolean createNode(String path, String data) {
        try {
            String str = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return true;
        } catch (Exception e) {
            log.error("创建持久化节点异常:{},{}", path, e);
            return false;
        }
    }

    /**
     * 修改节点
     *
     * @param path 节点路径
     * @param data 节点数据
     * @return 返回修改状态
     */
    public boolean updateNode(String path, String data) {
        try {
            //zk的数据版本是从0开始计数的。如果客户端传入的是-1，则表示zk服务器需要基于最新的数据进行更新。如果对zk的数据节点的更新操作没有原子性要求则可以使用-1.
            //version参数指定要更新的数据的版本, 如果version和真实的版本不同, 更新操作将失败. 指定version为-1则忽略版本检查
            zooKeeper.setData(path, data.getBytes(), -1);
            return true;
        } catch (Exception e) {
            log.error("修改持久化节点异常：{},{},{}", path, data, e);
            return false;
        }
    }

    /**
     * 删除节点
     *
     * @param path 节点路径
     * @return 返回装填
     */
    public boolean deleteNode(String path) {
        try {
            zooKeeper.delete(path, -1);
            return true;
        } catch (Exception e) {
            log.error("删除节点异常：{},{}", path, e);
        }
        return false;
    }

    /**
     * 获取当前节点所有子节点，不包含二级以下节点
     *
     * @param path 当前节点路径
     * @return 返回节点路径集合
     * @throws KeeperException      zk异常
     * @throws InterruptedException 中断异常
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(path, false);
    }

    /**
     * 获取当前节点数据
     *
     * @param path    当前节点路径
     * @param watcher 监听事件
     * @return 返回数据
     */
    public String getData(String path, Watcher watcher) {
        try {
            byte[] bytes = zooKeeper.getData(path, watcher, new Stat());
            return new String(bytes);
        } catch (Exception e) {
            log.error("获取节点数据异常：{},{}", path, e);
        }
        return null;
    }

    @PostConstruct
    public void init() {
        String path = "/kiss2";
        log.info("*************************执行初始化方法**********************************");
        boolean rus = createNode(path, "测试");
        String value = getData(path, watcherApi);
        log.info("数据为：{}", value);
        log.info("*************************END**********************************");
        deleteNode(path);
    }
}
