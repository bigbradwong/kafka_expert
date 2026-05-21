package com.example.service;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 优化版 K8s 分区注册表
 * 1. 启动时自动初始化 ConfigMap
 * 2. 移除冗余的获取逻辑
 */
@Component
public class K8sPartitionRegistry {

    private final KubernetesClient k8sClient = new KubernetesClientBuilder().build();
    private final String CM_NAME = "kafka-sse-partition-lock";
    private final String NAMESPACE = "default";

    @Value("${spring.kafka.consumer-group-prefix}")
    private String groupPrefix;

    /**
     * 服务启动时执行初始化：确保资源存在
     */
    @PostConstruct
    public void init() {
        ConfigMap cm = k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).get();
        if (cm == null) {
            cm = new ConfigMapBuilder()
                    .withNewMetadata().withName(CM_NAME).endMetadata()
                    .withData(new HashMap<>())
                    .build();
            k8sClient.configMaps().inNamespace(NAMESPACE).resource(cm).create();
        }
    }

    public String tryAssign(TopicPartition tp, String clientId) {
        String key = formatKey(tp);
        final String[] activeOwner = {null};

        // edit() 会自动处理获取最新版本、重试冲突等复杂逻辑
        k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).edit(c -> {
            Map<String, String> data = c.getData() == null ? new HashMap<>() : new HashMap<>(c.getData());
            if (data.containsKey(key)) {
                String val = data.get(key);
                if (!isStale(val)) {
                    String existingId = val.split("\\|")[0];
                    if (!existingId.equals(clientId)) {
                        activeOwner[0] = existingId;
                        return c;
                    }
                }
            }
            data.put(key, clientId + "|" + Instant.now().getEpochSecond());
            return new ConfigMapBuilder(c).withData(data).build();
        });
        return activeOwner[0];
    }

    public void heartbeat(TopicPartition tp, String clientId) {
        k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).edit(c -> {
            Map<String, String> data = c.getData() == null ? new HashMap<>() : new HashMap<>(c.getData());
            data.put(formatKey(tp), clientId + "|" + Instant.now().getEpochSecond());
            return new ConfigMapBuilder(c).withData(data).build();
        });
    }

    public void release(TopicPartition tp) {
        String key = formatKey(tp);
        k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).edit(c -> {
            Map<String, String> data = c.getData();
            if (data != null && data.containsKey(key)) {
                Map<String, String> newData = new HashMap<>(data);
                newData.remove(key);
                return new ConfigMapBuilder(c).withData(newData).build();
            }
            return c;
        });
    }

    private String formatKey(TopicPartition tp) {
        return groupPrefix + "-" + tp.topic() + "-" + tp.partition();
    }

    private boolean isStale(String val) {
        try {
            long ts = Long.parseLong(val.split("\\|")[1]);
            return (Instant.now().getEpochSecond() - ts) > 300;
        } catch (Exception e) { return true; }
    }
}
