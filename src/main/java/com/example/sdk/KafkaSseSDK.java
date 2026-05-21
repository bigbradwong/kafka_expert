package com.example.sdk;

import okhttp3.*;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONObject;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaSseSDK {
    private final KafkaSseConfig config;
    private final OkHttpClient client;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isFatalError = new AtomicBoolean(false);

    // OAuth2 缓存
    private String cachedToken = null;
    private long tokenExpiryTime = 0;

    private int totalPartitions = 0;
    private final AtomicInteger finishedCount = new AtomicInteger(0);
    private final Set<Integer> finishedPartitionIds = ConcurrentHashMap.newKeySet();

    public KafkaSseSDK(KafkaSseConfig config) {
        this.config = config;
        this.client = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    }

    /**
     * 获取或刷新 OAuth2 Access Token
     */
    private synchronized String getAccessToken() throws IOException {
        if (!config.isEnableOAuth2()) return null;

        // 如果 Token 还在有效期内（预留 60s 缓冲），直接返回
        if (cachedToken != null && System.currentTimeMillis() < tokenExpiryTime - 60000) {
            return cachedToken;
        }

        System.out.println("Fetching new OAuth2 access token...");
        FormBody formBody = new FormBody.Builder()
                .add("grant_type", "client_credentials")
                .add("client_id", config.getOauthClientId())
                .add("client_secret", config.getOauthClientSecret())
                .add("scope", config.getScope() != null ? config.getScope() : "")
                .build();

        Request request = new Request.Builder().url(config.getTokenUrl()).post(formBody).build();

        try (Response resp = client.newCall(request).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) {
                throw new IOException("OAuth2 token fetch failed: " + resp.code());
            }
            JSONObject json = new JSONObject(resp.body().string());
            this.cachedToken = json.getString("access_token");
            // expires_in 通常是秒
            long expiresIn = json.has("expires_in") ? json.getLong("expires_in") : 3600;
            this.tokenExpiryTime = System.currentTimeMillis() + (expiresIn * 1000);
            return cachedToken;
        }
    }

    /**
     * 构建带认证信息的请求构造器
     */
    private Request.Builder createAuthenticatedRequestBuilder(String url) throws IOException {
        Request.Builder builder = new Request.Builder().url(url);
        if (config.isEnableOAuth2()) {
            String token = getAccessToken();
            builder.header("Authorization", "Bearer " + token);
        }
        return builder;
    }

    public void startAuto() {
        if (isFatalError.get()) return;
        scheduler.execute(() -> {
            try {
                HttpUrl url = HttpUrl.parse(config.getServerUrl() + "/metadata").newBuilder()
                        .addQueryParameter("topic", config.getTopic())
                        .addQueryParameter("clientId", config.getClientId())
                        .build();

                Request req = createAuthenticatedRequestBuilder(url.toString()).build();
                try (Response resp = client.newCall(req).execute()) {
                    if (resp.isSuccessful() && resp.body() != null) {
                        JSONObject json = new JSONObject(resp.body().string());
                        this.totalPartitions = json.getInt("partitionCount");
                        for (int i = 0; i < totalPartitions; i++) startConsume(i, 0L);
                    } else {
                        handleFailure(null, resp, "Metadata discovery");
                    }
                }
            } catch (Exception e) {
                handleFailure(e, null, "Metadata discovery");
            }
        });
    }

    public void startConsume(int partition, long initialOffset) {
        if (isFatalError.get()) return;
        scheduler.execute(() -> runConsumeLoop(partition, initialOffset));
    }

    private void runConsumeLoop(int partition, long defaultOffset) {
        if (isFatalError.get()) return;

        long currentOffset = defaultOffset;
        if (config.isEnableExternalStore() && config.getOffsetStore() != null) {
            currentOffset = config.getOffsetStore().load(config.getTopic(), partition, defaultOffset);
        }

        HttpUrl httpUrl = HttpUrl.parse(config.getServerUrl() + "/stream").newBuilder()
                .addQueryParameter("topic", config.getTopic())
                .addQueryParameter("partition", String.valueOf(partition))
                .addQueryParameter("offset", String.valueOf(currentOffset))
                .addQueryParameter("mode", config.getMode().name())
                .addQueryParameter("clientId", config.getClientId())
                .build();

        try {
            Request request = createAuthenticatedRequestBuilder(httpUrl.toString())
                    .header("Accept", "text/event-stream")
                    .build();

            long finalOffset = currentOffset;
            EventSources.createFactory(client).newEventSource(request, new EventSourceListener() {
                private long lastSeenOffset = finalOffset;
                private final AtomicBoolean isFinished = new AtomicBoolean(false);

                @Override
                public void onEvent(@NotNull EventSource s, @Nullable String id, @Nullable String type, @NotNull String data) {
                    if (isFinished.get() || isFatalError.get()) return;
                    if ("sleep".equals(type)) {
                        stopThisStream(s);
                        scheduler.schedule(() -> runConsumeLoop(partition, lastSeenOffset), config.getSleepIntervalMin(), TimeUnit.MINUTES);
                    } else if ("complete".equals(type)) {
                        stopThisStream(s);
                        checkGlobalCompletion(partition);
                    } else if ("kafka-msg".equals(type)) {
                        config.getMessageHandler().accept(data);
                        if (id != null) {
                            lastSeenOffset = Long.parseLong(id) + 1;
                            if (config.isEnableExternalStore() && config.getOffsetStore() != null) {
                                config.getOffsetStore().save(config.getTopic(), partition, lastSeenOffset);
                            }
                        }
                    }
                }

                @Override
                public void onFailure(@NotNull EventSource s, @Nullable Throwable t, @Nullable Response r) {
                    if (isFinished.get() || isFatalError.get()) return;
                    stopThisStream(s);
                    // 如果收到 401 Unauthorized，强制清空 Token 缓存
                    if (r != null && r.code() == 401) {
                        cachedToken = null;
                        System.err.println("Token expired (401). Retrying with new token...");
                    }
                    if (isFatal(t, r)) markFatal(t, r, "P" + partition);
                    else scheduler.schedule(() -> runConsumeLoop(partition, lastSeenOffset), config.getRetryIntervalSec(), TimeUnit.SECONDS);
                }

                private void stopThisStream(EventSource s) {
                    s.cancel();
                    isFinished.set(true);
                }
            });
        } catch (IOException e) {
            handleFailure(e, null, "P" + partition + " init");
        }
    }

    private void checkGlobalCompletion(int partitionId) {
        if (config.getMode() != KafkaSseConfig.ConsumeMode.TASK) return;
        finishedPartitionIds.add(partitionId);
        if (finishedCount.incrementAndGet() >= totalPartitions && finishedPartitionIds.size() >= totalPartitions) {
            System.out.println(">>> TOPIC TASK COMPLETED.");
            shutdown();
        }
    }

    private boolean isFatal(Throwable t, Response r) {
        if (r != null && r.code() >= 400 && r.code() < 500) {
            // 401 和 408/429 除外
            return r.code() != 401 && r.code() != 408 && r.code() != 429;
        }
        return t instanceof UnknownHostException || t instanceof ConnectException;
    }

    private void markFatal(Throwable t, Response r, String context) {
        if (isFatalError.compareAndSet(false, true)) {
            String msg = (r != null) ? "HTTP " + r.code() : (t != null ? t.getMessage() : "Unknown");
            System.err.println("FATAL [" + context + "]: " + msg + ". Stopping SDK.");
            if (config.getErrorHandler() != null) config.getErrorHandler().accept(t != null ? t : new RuntimeException(msg));
            shutdown();
        }
    }

    private void handleFailure(Throwable t, Response r, String context) {
        if (isFatal(t, r)) markFatal(t, r, context);
        else scheduler.schedule(this::startAuto, config.getRetryIntervalSec(), TimeUnit.SECONDS);
    }

    public void shutdown() {
        scheduler.shutdown();
        client.dispatcher().executorService().shutdown();
    }
}
