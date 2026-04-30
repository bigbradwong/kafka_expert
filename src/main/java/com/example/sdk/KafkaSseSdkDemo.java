package com.example.sdk;

public class KafkaSseSdkDemo {
    public static void main(String[] args) {
        // 1. 初始化 SDK 配置
        KafkaSseConfig config = KafkaSseConfig.builder()
                .serverUrl("http://localhost:8080/api/v1/kafka")
                .topic("order_topic")
                .retryIntervalSec(10)      // 故障 10 秒重试
                .sleepIntervalMin(5)       // 冲突 5 分钟避让
                .enableExternalStore(true) // 开启外部位移同步
                .offsetStore(new LocalOffsetStore()) // 这里可以替换成 Redis/JDBC 实现
                .messageHandler(msg -> {
                    System.out.println("Processing: " + msg);
                })
                .build();

        // 2. 创建 SDK 实例
        KafkaSseSDK sdk = new KafkaSseSDK(config);

        // 3. 启动多个并发消费任务（针对不同 Partition）
        // 比如该 Topic 有 2 个分区
        sdk.startConsume(0, 0L); // 消费分区 0
        sdk.startConsume(1, 0L); // 消费分区 1
        
        System.out.println("SDK started successfully. Consuming partition 0 & 1...");
    }
}

/**
 * 演示用的内存位移存储实现
 */
class LocalOffsetStore implements OffsetStore {
    private final java.util.Map<String, Long> storage = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void save(String topic, int partition, long offset) {
        storage.put(topic + "-" + partition, offset);
    }

    @Override
    public long load(String topic, int partition, long defaultOffset) {
        return storage.getOrDefault(topic + "-" + partition, defaultOffset);
    }
}
