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

    // TASK 模式全局终结逻辑所需变量
    private int totalPartitions = 0;
    private final AtomicInteger finishedCount = new AtomicInteger(0);
    private final Set<Integer> finishedPartitionIds = ConcurrentHashMap.newKeySet();

    public KafkaSseSDK(KafkaSseConfig config) {
        this.config = config;
        this.client = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void startAuto() {
        if (isFatalError.get()) return;
        scheduler.execute(() -> {
            try {
                HttpUrl url = HttpUrl.parse(config.getServerUrl() + "/metadata").newBuilder()
                        .addQueryParameter("topic", config.getTopic())
                        .addQueryParameter("clientId", config.getClientId())
                        .build();

                Request req = new Request.Builder().url(url).build();
                try (Response resp = client.newCall(req).execute()) {
                    if (resp.isSuccessful() && resp.body() != null) {
                        JSONObject json = new JSONObject(resp.body().string());
                        this.totalPartitions = json.getInt("partitionCount");
                        System.out.println("Discovered " + totalPartitions + " partitions. Starting parallel consume...");
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

        Request request = new Request.Builder().url(httpUrl).header("Accept", "text/event-stream").build();

        long finalOffset = currentOffset;
        EventSources.createFactory(client).newEventSource(request, new EventSourceListener() {
            private long lastSeenOffset = finalOffset;
            private final AtomicBoolean isFinished = new AtomicBoolean(false);

            @Override
            public void onEvent(@NotNull EventSource s, @Nullable String id, @Nullable String type, @NotNull String data) {
                if (isFinished.get() || isFatalError.get()) return;

                if ("sleep".equals(type)) {
                    System.err.println("P" + partition + " locked. Sleeping...");
                    stopThisStream(s);
                    scheduler.schedule(() -> runConsumeLoop(partition, lastSeenOffset), config.getSleepIntervalMin(), TimeUnit.MINUTES);
                } else if ("complete".equals(type)) {
                    System.out.println("P" + partition + " signaled completion.");
                    stopThisStream(s);
                    checkGlobalCompletion(partition); // 触发全局判定
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
                if (isFatal(t, r)) markFatal(t, r, "P" + partition);
                else scheduler.schedule(() -> runConsumeLoop(partition, lastSeenOffset), config.getRetryIntervalSec(), TimeUnit.SECONDS);
            }

            private void stopThisStream(EventSource s) {
                s.cancel();
                isFinished.set(true);
            }
        });
    }

    /**
     * 双层保障的全局完成判定逻辑
     */
    private void checkGlobalCompletion(int partitionId) {
        if (config.getMode() != KafkaSseConfig.ConsumeMode.TASK) return;

        // 1. 记录分区 ID (利用 Set 的去重特性，防止某个分区重连后多次触发 complete)
        finishedPartitionIds.add(partitionId);
        
        // 2. 递增计数器
        int currentCount = finishedCount.incrementAndGet();

        // 3. 双层检查判定
        if (currentCount >= totalPartitions && finishedPartitionIds.size() >= totalPartitions) {
            System.out.println(">>> TOPIC TASK COMPLETED: All " + totalPartitions + " partitions finished.");
            shutdown();
        } else {
            System.out.println("Progress: " + finishedPartitionIds.size() + "/" + totalPartitions + " partitions done.");
        }
    }

    private boolean isFatal(Throwable t, Response r) {
        if (r != null && r.code() >= 400 && r.code() < 500) return r.code() != 408 && r.code() != 429;
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
