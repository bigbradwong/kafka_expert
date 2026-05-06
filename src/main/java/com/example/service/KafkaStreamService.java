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
import java.util.function.Consumer;

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

    public void pollAndStream(KafkaConsumer<String, String> consumer, SseEmitter emitter, 
                              TopicPartition tp, String clientIp, K8sPartitionRegistry registry) {
        try (consumer) {
            int emptyPolls = 0;
            long lastHeartbeat = 0;

            while (emptyPolls < 3) {
                // 每隔 60 秒执行一次 K8s 锁续约心跳
                if (Instant.now().getEpochSecond() - lastHeartbeat > 60) {
                    registry.heartbeat(tp, clientIp);
                    lastHeartbeat = Instant.now().getEpochSecond();
                    log.debug("Heartbeat sent for partition {}", tp.partition());
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    emptyPolls++;
                    emitter.send(SseEmitter.event().comment("hb-" + emptyPolls));
                    continue;
                }

                emptyPolls = 0;
                for (ConsumerRecord<String, String> record : records) {
                    String json = String.format("{\"partition\":%d,\"offset\":%d,\"value\":\"%s\"}", 
                            record.partition(), record.offset(), record.value().replace("\"", "\\\""));
                    emitter.send(SseEmitter.event().id(String.valueOf(record.offset())).name("kafka-msg").data(json));
                }
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
        return props;
    }
}
