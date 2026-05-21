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

    @Value("${spring.kafka.consumer-group-prefix}")
    private String groupPrefix;

    public int getPartitionCount(String topic) {
        Properties props = createBaseProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupPrefix + "-metadata-" + System.currentTimeMillis());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic);
            if (infos == null) throw new RuntimeException("Topic not found: " + topic);
            return infos.size();
        }
    }

    public KafkaConsumer<String, String> createAndVerify(String topic, int partition, long offset) {
        Properties props = createBaseProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupPrefix + "-" + topic + "-" + partition);
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(tp));
        
        long earliest = consumer.beginningOffsets(Collections.singletonList(tp)).get(tp);
        consumer.seek(tp, Math.max(offset, earliest));
        return consumer;
    }

    public void pollAndStream(KafkaConsumer<String, String> consumer, SseEmitter emitter, 
                              TopicPartition tp, String clientId, K8sPartitionRegistry registry, String mode) {
        try (consumer) {
            int emptyPolls = 0;
            long lastHeartbeat = 0;
            boolean isListening = "LISTENING".equalsIgnoreCase(mode);

            while (true) {
                if (Instant.now().getEpochSecond() - lastHeartbeat > 60) {
                    registry.heartbeat(tp, clientId);
                    lastHeartbeat = Instant.now().getEpochSecond();
                    emitter.send(SseEmitter.event().comment("heartbeat"));
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                
                if (records.isEmpty()) {
                    emptyPolls++;
                    if (!isListening && emptyPolls >= 3) {
                        break; 
                    }
                    continue;
                }

                emptyPolls = 0;
                for (ConsumerRecord<String, String> record : records) {
                    String json = String.format("{\"partition\":%d,\"offset\":%d,\"value\":\"%s\"}", 
                            record.partition(), record.offset(), record.value().replace("\"", "\\\""));
                    emitter.send(SseEmitter.event().id(String.valueOf(record.offset())).name("kafka-msg").data(json));
                }
            }
            
            if (!isListening) {
                registry.release(tp);
            }
            emitter.send(SseEmitter.event().name("complete").data("finished"));
            emitter.complete();
            
        } catch (Exception e) {
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

        // --- 跨国/长距离网络高性能调优参数 ---
        
        // 1. 批量配置：单次 poll 返回的最大条数。
        // 调大至 2000 条，配合 1MB 的抓取阈值，提升内存到网络的分发效率。
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000); 
        
        // 2. 最小拉取量：1MB。
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576); 
        
        // 3. 抓取等待：1000ms。
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        
        // 4. 单分区最大拉取量：5MB。
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 5242880);
        
        // 5. 请求超时：30秒。
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        return props;
    }
}
