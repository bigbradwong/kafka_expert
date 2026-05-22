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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 工业级 Kafka SSE SDK 核心 (JDK 1.8 兼容版)
 * 支持局部容错终结逻辑：单分区失败多次后视为完成，不阻塞全局 Task。
 */
public class KafkaSseSDK {
    private final KafkaSseConfig config;
    private final OkHttpClient client;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isFatalError = new AtomicBoolean(false);

    // OAuth2 缓存
    private String cachedToken = null;
    private long tokenExpiryTime = 0;

    // TASK 模式全局终结逻辑变量
    private volatile int totalPartitions = 0;
    private final AtomicInteger finishedCount = new AtomicInteger(0);
    private final Set<Integer> finishedPartitionIds = ConcurrentHashMap.newKeySet();
    private final Set<Integer> failedPartitionIds = ConcurrentHashMap.newKeySet();

    // 分区重试追踪器: PartitionId -> RetryCount
    private final ConcurrentHashMap<Integer, Integer> partitionRetryTracker = new ConcurrentHashMap<>();

    public KafkaSseSDK(KafkaSseConfig config) {
        this.config = config;
        this.client = new OkHttpClient.Builder()
                .readTimeout(config.getInactivityTimeoutSec(), TimeUnit.SECONDS) // 设置不活动超时
                .connectTimeout(10, TimeUnit.SECONDS)
                .build();
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    }

    private synchronized String getAccessToken() throws IOException {
        if (!config.isEnableOAuth2()) return null;
        if (cachedToken != null && System.currentTimeMillis() < tokenExpiryTime - 60000) return cachedToken;

        FormBody.Builder formBuilder = new FormBody.Builder()
                .add("grant_type", "client_credentials")
                .add("client_id", config.getOauthClientId())
                .add("client_secret", config.getOauthClientSecret());
        if (config.getScope() != null) formBuilder.add("scope", config.getScope());

        Request request = new Request.Builder().url(config.getTokenUrl()).post(formBuilder.build()).build();
        try (Response resp = client.newCall(request).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) throw new IOException("OAuth2 fetch failed: " + resp.code());
            JSONObject json = new JSONObject(resp.body().string());
            this.cachedToken = json.getString("access_token");
            long expiresIn = json.has("expires_in") ? json.getLong("expires_in") : 3600;
            this.tokenExpiryTime = System.currentTimeMillis() + (expiresIn * 1000);
            return cachedToken;
        }
    }

    private Request.Builder createAuthenticatedRequestBuilder(String url) throws IOException {
        Request.Builder builder = new Request.Builder().url(url);
        if (config.isEnableOAuth2()) {
            builder.header("Authorization", "Bearer " + getAccessToken());
        }
        return builder;
    }

    public void startAuto() {
        if (isFatalError.get()) return;
        scheduler.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    HttpUrl url = HttpUrl.parse(config.getServerUrl() + "/metadata").newBuilder()
                            .addQueryParameter("topic", config.getTopic())
                            .addQueryParameter("clientId", config.getClientId())
                            .build();

                    Request req = createAuthenticatedRequestBuilder(url.toString()).build();
                    try (Response resp = client.newCall(req).execute()) {
                        if (resp.isSuccessful() && resp.body() != null) {
                            JSONObject json = new JSONObject(resp.body().string());
                            totalPartitions = json.getInt("partitionCount");
                            System.out.println("Auto-discovery: " + totalPartitions + " partitions. Starting...");
                            for (int i = 0; i < totalPartitions; i++) startConsume(i, 0L);
                        } else {
                            handleGlobalFailure(null, resp, "Metadata discovery");
                        }
                    }
                } catch (Exception e) {
                    handleGlobalFailure(e, null, "Metadata discovery");
                }
            }
        });
    }

    public void startConsume(int partition, long initialOffset) {
        if (isFatalError.get()) return;
        scheduler.execute(() -> runConsumeLoop(partition, initialOffset));
    }

    private void runConsumeLoop(int partition, long defaultOffset) {
        if (isFatalError.get() || finishedPartitionIds.contains(partition)) return;

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

            final long finalOffset = currentOffset;
            EventSources.createFactory(client).newEventSource(request, new EventSourceListener() {
                private long lastSeenOffset = finalOffset;
                private final AtomicBoolean isFinished = new AtomicBoolean(false);

                @Override
                public void onEvent(@NotNull EventSource s, @Nullable String id, @Nullable String type, @NotNull String data) {
                    if (isFinished.get() || isFatalError.get()) return;

                    // 收到任何事件（包括心跳），均重置该分区的重试计数
                    partitionRetryTracker.remove(partition);

                    if ("sleep".equals(type)) {
                        stopThisStream(s);
                        System.err.println("P" + partition + " locked. Retrying later.");
                        scheduler.schedule(() -> runConsumeLoop(partition, lastSeenOffset), config.getSleepIntervalMin(), TimeUnit.MINUTES);
                    } else if ("complete".equals(type)) {
                        stopThisStream(s);
                        markPartitionFinished(partition, false);
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

                    if (r != null && r.code() == 401) cachedToken = null;

                    if (isFatal(t, r)) {
                        markFatal(t, r, "P" + partition);
                    } else {
                        handlePartitionFailure(partition, lastSeenOffset, t, r);
                    }
                }

                private void stopThisStream(EventSource s) {
                    s.cancel();
                    isFinished.set(true);
                }
            });
        } catch (IOException e) {
            handlePartitionFailure(partition, defaultOffset, e, null);
        }
    }

    /**
     * 处理单分区故障逻辑
     */
    private void handlePartitionFailure(int partition, long offset, Throwable t, Response r) {
        if (config.getMode() == KafkaSseConfig.ConsumeMode.LISTENING) {
            // 监听模式：无限重连
            scheduler.schedule(() -> runConsumeLoop(partition, offset), config.getRetryIntervalSec(), TimeUnit.SECONDS);
            return;
        }

        // TASK 模式：判断重试次数
        int retries = partitionRetryTracker.getOrDefault(partition, 0) + 1;
        if (retries > config.getMaxPartitionRetries()) {
            System.err.println("P" + partition + " failed after " + config.getMaxPartitionRetries() + " attempts. Marking as DONE (with ERROR).");
            partitionRetryTracker.remove(partition);
            markPartitionFinished(partition, true);
        } else {
            partitionRetryTracker.put(partition, retries);
            System.err.println("P" + partition + " retry " + retries + "/" + config.getMaxPartitionRetries() + " in " + config.getRetryIntervalSec() + "s...");
            scheduler.schedule(() -> runConsumeLoop(partition, offset), config.getRetryIntervalSec(), TimeUnit.SECONDS);
        }
    }

    private void markPartitionFinished(int partitionId, boolean isError) {
        if (isError) failedPartitionIds.add(partitionId);
        
        if (finishedPartitionIds.add(partitionId)) {
            int done = finishedCount.incrementAndGet();
            if (done >= totalPartitions) {
                System.out.println(">>> [FINISH] Task completed. Success: " + (totalPartitions - failedPartitionIds.size()) 
                        + ", Fail: " + failedPartitionIds.size());
                if (!failedPartitionIds.isEmpty()) {
                    System.err.println("Failed partitions: " + failedPartitionIds);
                }
                shutdown();
            } else {
                Set<Integer> remaining = IntStream.range(0, totalPartitions)
                        .boxed().filter(id -> !finishedPartitionIds.contains(id))
                        .collect(Collectors.toSet());
                System.out.println("Task Progress: " + done + "/" + totalPartitions + ". Pending: " + remaining);
            }
        }
    }

    private boolean isFatal(Throwable t, Response r) {
        if (r != null && r.code() >= 400 && r.code() < 500) {
            return r.code() != 401 && r.code() != 408 && r.code() != 429;
        }
        return t instanceof UnknownHostException || t instanceof ConnectException;
    }

    private void markFatal(Throwable t, Response r, String context) {
        if (isFatalError.compareAndSet(false, true)) {
            String msg = (r != null) ? "HTTP " + r.code() : (t != null ? t.getMessage() : "Unknown");
            System.err.println("FATAL [" + context + "]: " + msg + ". SDK stopping.");
            if (config.getErrorHandler() != null) {
                config.getErrorHandler().accept(t != null ? t : new RuntimeException("Fatal: " + msg));
            }
            shutdown();
        }
    }

    private void handleGlobalFailure(Throwable t, Response r, String context) {
        if (isFatal(t, r)) markFatal(t, r, context);
        else scheduler.schedule(this::startAuto, config.getRetryIntervalSec(), TimeUnit.SECONDS);
    }

    public void shutdown() {
        scheduler.shutdown();
        client.dispatcher().executorService().shutdown();
    }
}
