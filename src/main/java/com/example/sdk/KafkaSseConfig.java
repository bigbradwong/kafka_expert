package com.example.sdk;

import lombok.Builder;
import lombok.Data;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@Data @Builder
public class KafkaSseConfig {
    
    public enum ConsumeMode { TASK, LISTENING }

    private String serverUrl;
    private String topic;
    
    // 基础标识
    @Builder.Default private String clientId = "sdk-java-" + UUID.randomUUID().toString().substring(0, 8);
    @Builder.Default private ConsumeMode mode = ConsumeMode.TASK;
    
    // 重试与休眠配置
    @Builder.Default private int retryIntervalSec = 10;
    @Builder.Default private int sleepIntervalMin = 5;
    @Builder.Default private int inactivityTimeoutSec = 120;
    @Builder.Default private int maxPartitionRetries = 10;

    // 批量处理增强
    /** 是否开启批量消费模式。若开启，将调用 batchMessageHandler。 */
    @Builder.Default private boolean enableBatchMode = false;
    /** 批量消息处理器 */
    private Consumer<List<String>> batchMessageHandler;
    /** 单条消息处理器 */
    private Consumer<String> messageHandler;

    // 位移存储
    @Builder.Default private boolean enableExternalStore = false;
    private OffsetStore offsetStore;
    private Consumer<Throwable> errorHandler;

    // SSL 与 认证
    @Builder.Default private String trustedCertResourcePath = "certs/ca.crt";
    @Builder.Default private boolean enableOAuth2 = false;
    private String tokenUrl;
    private String oauthClientId;
    private String oauthClientSecret;
    private String scope;
}
