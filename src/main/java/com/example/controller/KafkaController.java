package com.example.controller;

import com.example.service.KafkaStreamService;
import com.example.service.K8sPartitionRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaController {

    @Autowired private KafkaStreamService kafkaService;
    @Autowired private K8sPartitionRegistry k8sRegistry;
    
    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * 元数据接口 (JDK 1.8 兼容写法)
     */
    @GetMapping("/metadata")
    public Map<String, Object> getMetadata(@RequestParam String topic) {
        int partitions = kafkaService.getPartitionCount(topic);
        Map<String, Object> result = new HashMap<>();
        result.put("topic", topic);
        result.put("partitionCount", partitions);
        return result;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @RequestParam String topic,
            @RequestParam int partition,
            @RequestParam long offset,
            @RequestParam String clientId,
            @RequestParam(defaultValue = "TASK") String mode) {
        
        TopicPartition tp = new TopicPartition(topic, partition);
        String activeOwner = k8sRegistry.tryAssign(tp, clientId);
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
                kafkaService.pollAndStream(consumer, emitter, tp, clientId, k8sRegistry, mode);
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }
}
