package com.example.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Slf4j
@Service
public class KafkaStreamService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public int getPartitionCount(String topic) {
        Properties props = createBaseProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metadata-fetcher-" + System.currentTimeMillis());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic);
            if (infos == null) throw new RuntimeException("Topic not found: " + topic);
            return infos.size();
        }
    }

    public KafkaConsumer<String, String> createAndVerify(String topic, int partition, long offset) {
        Properties props = createBaseProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sse-group-" + topic + "-" + partition);
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(tp));
        
        long earliest = consumer.beginningOffsets(Collections.singletonList(tp)).get(tp);
        consumer.seek(tp, Math.max(offset, earliest));
        return consumer;
    }

    /**
     * @param mode "TASK" 或 "LISTENING"
     */
    public void pollAndStream(KafkaConsumer<String, String> consumer, SseEmitter emitter, 
                              TopicPartition tp, String clientIp, K8sPartitionRegistry registry, String mode) {
        try (consumer) {
            int emptyPolls = 0;
            long lastHeartbeat = 0;
            boolean isListening = "LISTENING".equalsIgnoreCase(mode);

            // 只要不是监听模式且空拉取达到3次，或者连接断开，就继续
            while (true) {
                // 1. 心跳与续约逻辑
                if (Instant.now().getEpochSecond() - lastHeartbeat > 60) {
                    registry.heartbeat(tp, clientIp);
                    lastHeartbeat = Instant.now().getEpochSecond();
                    emitter.send(SseEmitter.event().comment("heartbeat"));
                }

                // 2. 拉取消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                
                if (records.isEmpty()) {
                    emptyPolls++;
                    // TASK 模式下的退出机制
                    if (!isListening && emptyPolls >= 3) {
                        log.info("Task mode: No more data for P{}. Completing.", tp.partition());
                        break;
                    }
                    continue;
                }

                // 3. 处理消息
                emptyPolls = 0;
                for (ConsumerRecord<String, String> record : records) {
                    String json = String.format("{\"partition\":%d,\"offset\":%d,\"value\":\"%s\"}", 
                            record.partition(), record.offset(), record.value().replace("\"", "\\\""));
                    emitter.send(SseEmitter.event().id(String.valueOf(record.offset())).name("kafka-msg").data(json));
                }
            }
            
            // 只有退出循环（即 TASK 模式完成）才发 complete
            emitter.send(SseEmitter.event().name("complete").data("finished"));
            emitter.complete();
            
        } catch (Exception e) {
            log.error("Stream interrupted for P{}", tp.partition(), e);
            emitter.completeWithError(e);
        }
    }

    private Properties createBaseProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
