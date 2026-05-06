package com.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KafkaHeartbeatTest {

    @Test
    public void testHeartbeatTriggeredInPollLoop() throws Exception {
        // 1. 准备 Mocks
        KafkaStreamService service = new KafkaStreamService();
        KafkaConsumer<String, String> mockConsumer = mock(KafkaConsumer.class);
        SseEmitter mockEmitter = mock(SseEmitter.class);
        K8sPartitionRegistry mockRegistry = mock(K8sPartitionRegistry.class);
        TopicPartition tp = new TopicPartition("test-topic", 0);
        String clientIp = "127.0.0.1";

        // 模拟 poll 总是返回空，以便我们可以控制循环次数
        when(mockConsumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        // 使用 Mockito 模拟静态方法 Instant.now() 来控制时间流逝
        try (MockedStatic<Instant> mockedInstant = mockStatic(Instant.class)) {
            // 设置初始时间
            Instant start = Instant.ofEpochSecond(1000);
            Instant after70Seconds = Instant.ofEpochSecond(1070);
            Instant after140Seconds = Instant.ofEpochSecond(1140);

            // 定义时间序列
            mockedInstant.when(Instant::now).thenReturn(start, start, after70Seconds, after70Seconds, after140Seconds);

            // 2. 执行 pollAndStream (它会在 emptyPolls >= 3 时退出)
            // 循环逻辑：
            // 第 1 次 poll (T=1000): 满足心跳条件 (1000-0 > 60), 调用 heartbeat, emptyPolls=1
            // 第 2 次 poll (T=1070): 满足心跳条件 (1070-1000 > 60), 调用 heartbeat, emptyPolls=2
            // 第 3 次 poll (T=1140): 满足心跳条件 (1140-1070 > 60), 调用 heartbeat, emptyPolls=3 -> 退出
            service.pollAndStream(mockConsumer, mockEmitter, tp, clientIp, mockRegistry);

            // 3. 验证心跳是否被调用了 3 次
            verify(mockRegistry, times(3)).heartbeat(eq(tp), eq(clientIp));
            
            // 验证是否发送了 complete 事件
            verify(mockEmitter, times(1)).complete();
        }
    }
}
