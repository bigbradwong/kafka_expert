package com.example.service;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分区注册表（本地内存实现）
 * 逻辑与 K8sPartitionRegistry 保持一致，支持过期判定和心跳续约
 */
@Component
public class PartitionRegistry {

    // 存储格式: key -> "clientIp|timestamp"
    private final ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();

    /**
     * 尝试锁定分区
     * @return null 表示锁定成功；非 null 表示返回当前占用者信息
     */
    public synchronized String tryAssign(TopicPartition tp, String clientIp) {
        String key = formatKey(tp);
        if (registry.containsKey(key)) {
            String val = registry.get(key);
            if (!isStale(val)) {
                return val; // 锁仍有效，返回占用者
            }
        }
        
        // 抢占或续期
        updateEntry(key, clientIp);
        return null;
    }

    /**
     * 心跳续约
     */
    public void heartbeat(TopicPartition tp, String clientIp) {
        updateEntry(formatKey(tp), clientIp);
    }

    /**
     * 释放锁
     */
    public void release(TopicPartition tp) {
        registry.remove(formatKey(tp));
    }

    /**
     * 更新位移（保留接口兼容性）
     */
    public void updateOffset(TopicPartition tp, long offset) {
        // 逻辑合并至 heartbeat，通过刷新时间戳维系锁
    }

    private void updateEntry(String key, String clientIp) {
        registry.put(key, clientIp + "|" + Instant.now().getEpochSecond());
    }

    private String formatKey(TopicPartition tp) {
        return tp.topic() + "-" + tp.partition();
    }

    private boolean isStale(String val) {
        try {
            long ts = Long.parseLong(val.split("\\|")[1]);
            // 5分钟失效阈值
            return (Instant.now().getEpochSecond() - ts) > 300;
        } catch (Exception e) {
            return true;
        }
    }
}
