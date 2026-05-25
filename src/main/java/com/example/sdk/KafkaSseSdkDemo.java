package com.example.sdk;

import org.json.JSONObject;
import java.util.List;

/**
 * SDK 批量模式演示 Demo (JDK 1.8 兼容)
 */
public class KafkaSseSdkDemo {
    public static void main(String[] args) {
        // 1. 初始化 SDK 配置
        KafkaSseConfig config = KafkaSseConfig.builder()
                .serverUrl("http://localhost:8080/api/v1/kafka")
                .topic("order_topic")
                .clientId("demo-client-01")
                .mode(KafkaSseConfig.ConsumeMode.TASK) // 单次任务模式
                
                // 开启批量模式并注册处理器
                .enableBatchMode(true)
                .batchMessageHandler(new java.util.function.Consumer<List<String>>() {
                    @Override
                    public void accept(List<String> batch) {
                        System.out.println(String.format(">>> Processing Batch: %d messages", batch.size()));
                        for (String json : batch) {
                            try {
                                JSONObject msg = new JSONObject(json);
                                // 提取并打印 value 字段
                                String value = msg.getString("value");
                                long offset = msg.getLong("offset");
                                int partition = msg.getInt("partition");
                                System.out.println(String.format("  [P%d @ %d] Value: %s", partition, offset, value));
                            } catch (Exception e) {
                                System.err.println("  Parse error for message: " + json);
                            }
                        }
                    }
                })
                
                .retryIntervalSec(10)
                .enableExternalStore(true)
                .offsetStore(new LocalFileOffsetStore("./data/sdk-offsets")) // 使用 JDK 1.8 兼容的文件存储
                .build();

        // 2. 创建 SDK 实例
        KafkaSseSDK sdk = new KafkaSseSDK(config);

        // 3. 启动全分区自动消费
        System.out.println("SDK Batch Mode initiated. Auto-detecting partitions...");
        sdk.startAuto();
    }
}
