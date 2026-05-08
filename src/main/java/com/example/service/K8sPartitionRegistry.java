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

@Component
public class K8sPartitionRegistry {

    private final KubernetesClient k8sClient = new KubernetesClientBuilder().build();
    private final String CM_NAME = "kafka-sse-partition-lock";
    private final String NAMESPACE = "default";

    public synchronized String tryAssign(TopicPartition tp, String clientId) {
        String key = formatKey(tp);
        ConfigMap cm = getOrCreateConfigMap();
        Map<String, String> data = cm.getData() == null ? new HashMap<>() : cm.getData();

        if (data.containsKey(key)) {
            String val = data.get(key); // "clientId|timestamp"
            if (!isStale(val)) {
                String existingClientId = val.split("\\|")[0];
                if (existingClientId.equals(clientId)) return null;
                return existingClientId; 
            }
        }

        updateEntry(data, key, clientId);
        return null; 
    }

    public void heartbeat(TopicPartition tp, String clientId) {
        ConfigMap cm = getOrCreateConfigMap();
        Map<String, String> data = cm.getData() == null ? new HashMap<>() : cm.getData();
        updateEntry(data, formatKey(tp), clientId);
    }

    public void release(TopicPartition tp) {
        String key = formatKey(tp);
        ConfigMap cm = getOrCreateConfigMap();
        Map<String, String> data = cm.getData();
        if (data != null && data.containsKey(key)) {
            data.remove(key);
            updateConfigMap(data);
        }
    }

    private void updateEntry(Map<String, String> data, String key, String clientId) {
        data.put(key, clientId + "|" + Instant.now().getEpochSecond());
        updateConfigMap(data);
    }

    private String formatKey(TopicPartition tp) {
        return tp.topic() + "-" + tp.partition();
    }

    private void updateConfigMap(Map<String, String> data) {
        k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME)
                .edit(c -> new ConfigMapBuilder(c).withData(data).build());
    }

    private boolean isStale(String val) {
        try {
            long ts = Long.parseLong(val.split("\\|")[1]);
            return (Instant.now().getEpochSecond() - ts) > 300;
        } catch (Exception e) { return true; }
    }

    private ConfigMap getOrCreateConfigMap() {
        ConfigMap cm = k8sClient.configMaps().inNamespace(NAMESPACE).withName(CM_NAME).get();
        if (cm == null) {
            cm = new ConfigMapBuilder().withNewMetadata().withName(CM_NAME).endMetadata().withData(new HashMap<>()).build();
            k8sClient.configMaps().inNamespace(NAMESPACE).create(cm);
        }
        return cm;
    }
}
