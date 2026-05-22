package com.example.service;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Component
public class K8sPartitionRegistry {

    private final KubernetesClient k8sClient = new KubernetesClientBuilder().build();
    private final String CM_NAME = "kafka-sse-partition-lock";
    private final String NAMESPACE = "default";
    private final Random random = new Random();

    @Value("${spring.kafka.consumer-group-prefix}")
    private String groupPrefix;

    @PostConstruct
    public void init() {
        try {
            ConfigMap cm = k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).get();
            if (cm == null) {
                cm = new ConfigMapBuilder()
                        .withNewMetadata().withName(CM_NAME).endMetadata()
                        .withData(new HashMap<>())
                        .build();
                k8sClient.configMaps().inNamespace(NAMESPACE).resource(cm).create();
            }
        } catch (Exception e) {
            System.err.println("Initial ConfigMap check failed: " + e.getMessage());
        }
    }

    public String tryAssign(TopicPartition tp, String clientId) {
        String key = formatKey(tp);
        final String[] activeOwner = {null};

        executeWithRetry(() -> {
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
        });
        return activeOwner[0];
    }

    public void heartbeat(TopicPartition tp, String clientId) {
        executeWithRetry(() -> {
            k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).edit(c -> {
                Map<String, String> data = c.getData() == null ? new HashMap<>() : new HashMap<>(c.getData());
                data.put(formatKey(tp), clientId + "|" + Instant.now().getEpochSecond());
                return new ConfigMapBuilder(c).withData(data).build();
            });
        });
    }

    public void release(TopicPartition tp) {
        String key = formatKey(tp);
        executeWithRetry(() -> {
            k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).edit(c -> {
                Map<String, String> data = c.getData();
                if (data != null && data.containsKey(key)) {
                    Map<String, String> newData = new HashMap<>(data);
                    newData.remove(key);
                    return new ConfigMapBuilder(c).withData(newData).build();
                }
                return c;
            });
        });
    }

    /**
     * 专门针对 409 Conflict 增加的手动退避重试逻辑
     */
    private void executeWithRetry(Runnable action) {
        int maxAttempts = 10;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                action.run();
                return;
            } catch (KubernetesClientException e) {
                if (e.getCode() == 409 && i < maxAttempts - 1) {
                    try { Thread.sleep(50 + random.nextInt(150)); } catch (InterruptedException ignored) {}
                } else throw e;
            }
        }
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
