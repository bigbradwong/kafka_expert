package com.example.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

@Slf4j
@Service
public class KafkaStreamService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaConsumer<String, String> createAndVerify(String topic, int partition, long offset) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sse-group-" + topic + "-" + partition);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(tp));
        
        // 位移自动校准逻辑
        long earliest = consumer.beginningOffsets(Collections.singletonList(tp)).get(tp);
        if (offset < earliest) {
            log.warn("Offset {} is before earliest {}. Snapping to earliest.", offset, earliest);
            consumer.seek(tp, earliest);
        } else {
            consumer.seek(tp, offset);
        }
        return consumer;
    }

    public void pollAndStream(KafkaConsumer<String, String> consumer, SseEmitter emitter, 
                              TopicPartition tp, Consumer<Long> offsetUpdater) {
        try (consumer) {
            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                
                if (records.isEmpty()) {
                    emptyPolls++;
                    emitter.send(SseEmitter.event().comment("heartbeat-" + emptyPolls));
                    continue;
                }

                emptyPolls = 0;
                for (ConsumerRecord<String, String> record : records) {
                    String json = String.format("{\"partition\":%d,\"offset\":%d,\"value\":\"%s\"}", 
                            record.partition(), record.offset(), record.value().replace("\"", "\\\""));
                    
                    // 使用 offset 作为 SSE 的 ID，方便断点续传
                    emitter.send(SseEmitter.event()
                            .id(String.valueOf(record.offset()))
                            .name("kafka-msg")
                            .data(json));
                    
                    offsetUpdater.accept(record.offset());
                }
            }
            // 数据消费完毕发送结束事件
            emitter.send(SseEmitter.event().name("complete").data("finished"));
            emitter.complete();
        } catch (Exception e) {
            log.error("Streaming error for partition {}", tp.partition(), e);
            emitter.completeWithError(e);
        }
    }
}
