package com.example.service;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于 K8s ConfigMap 的分布式分区注册表
 * 允许多个服务端 Pod 共享分区锁定状态
 */
@Component
public class K8sPartitionRegistry {

    private final KubernetesClient k8sClient = new KubernetesClientBuilder().build();
    private final String CM_NAME = "kafka-sse-partition-lock";
    private final String NAMESPACE = "default";

    public synchronized String tryAssign(TopicPartition tp, String clientIp) {
        String key = tp.topic() + "-" + tp.partition();
        ConfigMap cm = getOrCreateConfigMap();
        Map<String, String> data = cm.getData() == null ? new HashMap<>() : cm.getData();

        if (data.containsKey(key)) {
            String val = data.get(key);
            if (!isStale(val)) {
                return val; // 锁有效，返回当前占用者
            }
        }

        // 抢占锁：写入 "ip|timestamp"
        data.put(key, clientIp + "|" + Instant.now().getEpochSecond());
        updateConfigMap(data);
        return null; // 成功
    }

    public void release(TopicPartition tp) {
        String key = tp.topic() + "-" + tp.partition();
        ConfigMap cm = getOrCreateConfigMap();
        Map<String, String> data = cm.getData();
        if (data != null && data.containsKey(key)) {
            data.remove(key);
            updateConfigMap(data);
        }
    }

    private void updateConfigMap(Map<String, String> data) {
        k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME)
                .edit(c -> new ConfigMapBuilder(c).withData(data).build());
    }

    private boolean isStale(String val) {
        // 策略：如果 5 分钟没有更新，则认为占用者已下线
        try {
            long ts = Long.parseLong(val.split("\\|")[1]);
            return (Instant.now().getEpochSecond() - ts) > 300;
        } catch (Exception e) { return true; }
    }

    private ConfigMap getOrCreateConfigMap() {
        ConfigMap cm = k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).get();
        if (cm == null) {
            cm = new ConfigMapBuilder()
                    .withNewMetadata().withName(CM_NAME).endMetadata()
                    .withData(new HashMap<>())
                    .build();
            k8sClient.configMaps().inNamespace(NAMESPACE).create(cm);
        }
        return cm;
    }
}
