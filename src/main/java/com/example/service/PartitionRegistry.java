package com.example.service;

import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分区注册表：实现轻量级的分布式锁模拟
 */
@Component
public class PartitionRegistry {
    
    @Data @Builder
    public static class AssignmentInfo {
        private String clientIp;
        private LocalDateTime assignTime;
        private long lastOffset;
    }

    private final ConcurrentHashMap<TopicPartition, AssignmentInfo> registry = new ConcurrentHashMap<>();

    /**
     * 尝试分配分区
     * @return null 表示分配成功；非 null 表示该分区已被他人占用，返回占用者信息
     */
    public synchronized AssignmentInfo tryAssign(TopicPartition tp, String ip, long initialOffset) {
        if (registry.containsKey(tp)) {
            return registry.get(tp);
        }
        AssignmentInfo info = AssignmentInfo.builder()
                .clientIp(ip)
                .assignTime(LocalDateTime.now())
                .lastOffset(initialOffset)
                .build();
        registry.put(tp, info);
        return null;
    }

    /**
     * 释放分区锁
     */
    public void release(TopicPartition tp) {
        registry.remove(tp);
    }

    /**
     * 更新最新的消费位移进度
     */
    public void updateOffset(TopicPartition tp, long offset) {
        AssignmentInfo info = registry.get(tp);
        if (info != null) {
            info.setLastOffset(offset);
        }
    }
}
