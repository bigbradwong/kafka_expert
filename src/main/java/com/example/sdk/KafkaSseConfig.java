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
    
    // 基础标识
    @Builder.Default private String clientId = "sdk-java-" + UUID.randomUUID().toString().substring(0, 8);
    @Builder.Default private ConsumeMode mode = ConsumeMode.TASK;
    
    // 重试与休眠配置
    @Builder.Default private int retryIntervalSec = 10;
    @Builder.Default private int sleepIntervalMin = 5;
    
    /** 
     * 不活动超时时间（秒）。如果超过此时间没有收到任何消息或心跳，SDK 将主动重连。
     * 建议设为比服务端心跳间隔稍长（如 120 秒）。
     */
    @Builder.Default private int inactivityTimeoutSec = 120;

    /**
     * 单分区最大重试次数。仅在 TASK 模式下生效。
     * 超过此次数后，该分区将被标记为 FAILED 并视为“已完成”，不再阻塞全局任务。
     */
    @Builder.Default private int maxPartitionRetries = 10;

    // 位移存储
    @Builder.Default private boolean enableExternalStore = false;
    private OffsetStore offsetStore;
    private Consumer<String> messageHandler;
    private Consumer<Throwable> errorHandler;

    /** 
     * 信任证书路径。
     * 默认为 "certs/ca.crt"，SDK 会尝试从 JAR 包内置资源中加载。
     */
    @Builder.Default private String trustedCertResourcePath = "certs/ca.crt";

    // OAuth2 认证配置
    @Builder.Default private boolean enableOAuth2 = false;
    private String tokenUrl;
    private String oauthClientId;
    private String oauthClientSecret;
    private String scope;
}
