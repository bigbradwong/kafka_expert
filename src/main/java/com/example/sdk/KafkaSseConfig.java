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
    
    @Builder.Default private String clientId = "sdk-java-" + UUID.randomUUID().toString().substring(0, 8);
    @Builder.Default private ConsumeMode mode = ConsumeMode.TASK;
    @Builder.Default private int retryIntervalSec = 10;
    @Builder.Default private int sleepIntervalMin = 5;
    @Builder.Default private boolean enableExternalStore = false;
    private OffsetStore offsetStore;
    private Consumer<String> messageHandler;
    
    /** 
     * 错误处理回调。
     * 当遇到致命错误或多次重试失败后，SDK 会通过此接口报告异常。
     */
    private Consumer<Throwable> errorHandler;
}
