package com.example.controller;

import com.example.service.KafkaStreamService;
import com.example.service.K8sPartitionRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaController {

    @Autowired private KafkaStreamService kafkaService;
    @Autowired private K8sPartitionRegistry k8sRegistry;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * 元数据接口：返回 Topic 的分区数量
     */
    @GetMapping("/metadata")
    public Map<String, Object> getMetadata(@RequestParam String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            var result = admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get();
            int partitions = result.get(topic).partitions().size();
            return Map.of("topic", topic, "partitionCount", partitions);
        } catch (Exception e) {
            throw new RuntimeException("Could not fetch kafka metadata: " + e.getMessage());
        }
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @RequestParam String topic,
            @RequestParam int partition,
            @RequestParam long offset,
            HttpServletRequest request) {
        
        TopicPartition tp = new TopicPartition(topic, partition);
        String clientIp = request.getRemoteAddr();
        
        // 使用 K8s 分布式注册表尝试锁定
        String activeOwner = k8sRegistry.tryAssign(tp, clientIp);
        if (activeOwner != null) {
            SseEmitter sleepEmitter = new SseEmitter(10_000L);
            try {
                sleepEmitter.send(SseEmitter.event().name("sleep").data(activeOwner));
                sleepEmitter.complete();
            } catch (Exception e) {}
            return sleepEmitter;
        }

        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> k8sRegistry.release(tp));
        emitter.onError(e -> k8sRegistry.release(tp));

        executor.execute(() -> {
            try {
                var consumer = kafkaService.createAndVerify(topic, partition, offset);
                kafkaService.pollAndStream(consumer, emitter, tp, (o) -> {
                    // 可以在这里定期更新 K8s CM 中的位移和时间戳以维持锁
                });
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }
}
