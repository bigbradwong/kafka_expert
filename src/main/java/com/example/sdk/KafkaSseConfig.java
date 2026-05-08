package com.example.sdk;

import lombok.Builder;
import lombok.Data;
import java.util.UUID;
import java.util.function.Consumer;

@Data @Builder
public class KafkaSseConfig {
    
    public enum ConsumeMode { TASK, LISTENING }

    private String serverUrl;
    private String topic;
    
    /** 
     * 客户端唯一标识。建议在 K8s 中使用 Pod Name。
     * 若不指定，将自动生成 UUID。
     */
    @Builder.Default private String clientId = "sdk-java-" + UUID.randomUUID().toString().substring(0, 8);
    
    @Builder.Default private ConsumeMode mode = ConsumeMode.TASK;
    @Builder.Default private int retryIntervalSec = 10;
    @Builder.Default private int sleepIntervalMin = 5;
    @Builder.Default private boolean enableExternalStore = false;
    private OffsetStore offsetStore;
    private Consumer<String> messageHandler;
}
