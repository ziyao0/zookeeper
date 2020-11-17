package com.kiss.zookeeper.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.springframework.stereotype.Component;

/**
 * 事件监听
 *
 * @author zhangziyao
 * @see Watcher
 */
@Slf4j
@Component
public class WatcherApi implements Watcher {

    @Override
    public void process(WatchedEvent event) {
        log.info("Watcher监听事件={}", event.getState());
        log.info("监听路径为={}", event.getPath());
        log.info("监听的类型为={}", event.getType());
    }
}
