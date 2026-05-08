package com.example.service;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分区注册表（本地内存版）
 * 改用 clientId 作为唯一标识，解决多 Pod 同 IP 导致的锁失效问题
 */
@Component
public class PartitionRegistry {

    private final ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();

    public synchronized String tryAssign(TopicPartition tp, String clientId) {
        String key = formatKey(tp);
        if (registry.containsKey(key)) {
            String val = registry.get(key); // 格式: "clientId|timestamp"
            if (!isStale(val)) {
                String existingClientId = val.split("\\|")[0];
                // 如果是同一个 clientId 重连，允许通过
                if (existingClientId.equals(clientId)) return null;
                return existingClientId; // 返回当前的占用者 ID
            }
        }
        updateEntry(key, clientId);
        return null;
    }

    public void heartbeat(TopicPartition tp, String clientId) {
        updateEntry(formatKey(tp), clientId);
    }

    public void release(TopicPartition tp) {
        registry.remove(formatKey(tp));
    }

    private void updateEntry(String key, String clientId) {
        registry.put(key, clientId + "|" + Instant.now().getEpochSecond());
    }

    private String formatKey(TopicPartition tp) {
        return tp.topic() + "-" + tp.partition();
    }

    private boolean isStale(String val) {
        try {
            long ts = Long.parseLong(val.split("\\|")[1]);
            return (Instant.now().getEpochSecond() - ts) > 300;
        } catch (Exception e) { return true; }
    }
}
