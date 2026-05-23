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
import java.util.stream.Collectors;

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
        boolean isListening = "LISTENING".equalsIgnoreCase(mode);
        try (consumer) {
            int emptyPolls = 0;
            long lastHeartbeat = 0;

            while (true) {
                if (Instant.now().getEpochSecond() - lastHeartbeat > 60) {
                    registry.heartbeat(tp, clientId);
                    lastHeartbeat = Instant.now().getEpochSecond();
                    emitter.send(SseEmitter.event().comment("heartbeat"));
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    emptyPolls++;
                    if (!isListening && emptyPolls >= 3) break; 
                    continue;
                }

                emptyPolls = 0;
                
                // --- 核心优化：批量打包 ---
                List<String> batchJson = new ArrayList<>();
                long lastOffset = -1;
                for (ConsumerRecord<String, String> record : records) {
                    batchJson.add(String.format("{\"partition\":%d,\"offset\":%d,\"value\":\"%s\"}", 
                            record.partition(), record.offset(), record.value().replace("\"", "\\\"")));
                    lastOffset = record.offset();
                }

                // 将整个 Poll 批次作为一个 JSON 数组发送
                String data = "[" + String.join(",", batchJson) + "]";
                emitter.send(SseEmitter.event()
                        .id(String.valueOf(lastOffset))
                        .name("kafka-msg-batch")
                        .data(data));
            }
            
            if (!isListening) {
                try { registry.release(tp); } catch (Exception e) {}
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
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000); 
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576); 
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 5242880);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return props;
    }
}
