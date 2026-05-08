package com.example.controller;

import com.example.service.KafkaStreamService;
import com.example.service.K8sPartitionRegistry;
import com.example.service.PartitionRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaController {

    @Autowired private KafkaStreamService kafkaService;
    // 注入 K8s 版或本地版。此处为演示方便同时保留，实际生产可根据环境 Profile 切换。
    @Autowired private K8sPartitionRegistry k8sRegistry; 
    
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @GetMapping("/metadata")
    public Map<String, Object> getMetadata(@RequestParam String topic) {
        int partitions = kafkaService.getPartitionCount(topic);
        return Map.of("topic", topic, "partitionCount", partitions);
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @RequestParam String topic,
            @RequestParam int partition,
            @RequestParam long offset,
            @RequestParam String clientId, // 核心修改：显式接收客户端 ID
            @RequestParam(defaultValue = "TASK") String mode) {
        
        TopicPartition tp = new TopicPartition(topic, partition);
        
        // 使用 clientId 尝试锁定分区
        String activeOwner = k8sRegistry.tryAssign(tp, clientId);
        if (activeOwner != null) {
            SseEmitter sleepEmitter = new SseEmitter(10_000L);
            try {
                // 返回占用者的 clientId
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
                // 使用 clientId 执行心跳续约
                kafkaService.pollAndStream(consumer, emitter, tp, clientId, k8sRegistry, mode);
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }
}
