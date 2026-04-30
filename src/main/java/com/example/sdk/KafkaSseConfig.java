package com.example.sdk;

import lombok.Builder;
import lombok.Data;

import java.util.function.Consumer;

@Data @Builder
public class KafkaSseConfig {
    /** 服务端基础地址，如 http://localhost:8080/api/v1/kafka */
    private String serverUrl;
    
    /** 目标 Topic */
    private String topic;
    
    /** 故障重试间隔（秒），默认 10 秒 */
    @Builder.Default private int retryIntervalSec = 10;
    
    /** 分区冲突避让休眠间隔（分钟），默认 5 分钟 */
    @Builder.Default private int sleepIntervalMin = 5;
    
    /** 是否开启外部位移存储，实现分布式容错 */
    @Builder.Default private boolean enableExternalStore = false;
    
    /** 外部存储实现类（JDBC, Redis, S3 等） */
    private OffsetStore offsetStore;
    
    /** 消息回调处理函数 */
    private Consumer<String> messageHandler;
}
