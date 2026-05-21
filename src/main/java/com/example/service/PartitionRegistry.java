package com.example.service;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分区注册表（本地内存实现）
 * 支持 consumer-group-prefix
 */
@Component
public class PartitionRegistry {

    private final ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();

    @Value("${spring.kafka.consumer-group-prefix}")
    private String groupPrefix;

    public synchronized String tryAssign(TopicPartition tp, String clientId) {
        String key = formatKey(tp);
        if (registry.containsKey(key)) {
            String val = registry.get(key);
            if (!isStale(val)) {
                String existingClientId = val.split("\\|")[0];
                if (existingClientId.equals(clientId)) return null;
                return existingClientId;
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
        // 与 K8s 注册表保持一致的 Key 格式
        return groupPrefix + "-" + tp.topic() + "-" + tp.partition();
    }

    private boolean isStale(String val) {
        try {
            long ts = Long.parseLong(val.split("\\|")[1]);
            return (Instant.now().getEpochSecond() - ts) > 300;
        } catch (Exception e) { return true; }
    }
}
