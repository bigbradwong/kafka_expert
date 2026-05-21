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
    
    // 位移存储
    @Builder.Default private boolean enableExternalStore = false;
    private OffsetStore offsetStore;
    private Consumer<String> messageHandler;
    private Consumer<Throwable> errorHandler;

    // OAuth2 认证配置
    @Builder.Default private boolean enableOAuth2 = false;
    private String tokenUrl;      // OAuth2 Token 获取地址
    private String oauthClientId; // OAuth2 客户端 ID (注意：这不同于上面的实例 ID)
    private String oauthClientSecret;
    private String scope;
}
