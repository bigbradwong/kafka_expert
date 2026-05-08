package com.example.sdk;

import okhttp3.*;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaSseSDK {
    private final KafkaSseConfig config;
    private final OkHttpClient client;
    private final ScheduledExecutorService scheduler;

    public KafkaSseSDK(KafkaSseConfig config) {
        this.config = config;
        this.client = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void startAuto() {
        scheduler.execute(() -> {
            try {
                // 元数据请求也带上 clientId 以便审计或限流
                HttpUrl url = HttpUrl.parse(config.getServerUrl() + "/metadata").newBuilder()
                        .addQueryParameter("topic", config.getTopic())
                        .addQueryParameter("clientId", config.getClientId())
                        .build();

                Request req = new Request.Builder().url(url).build();
                try (Response resp = client.newCall(req).execute()) {
                    if (resp.isSuccessful() && resp.body() != null) {
                        JSONObject json = new JSONObject(resp.body().string());
                        int partitionCount = json.getInt("partitionCount");
                        for (int i = 0; i < partitionCount; i++) {
                            startConsume(i, 0L);
                        }
                    } else {
                        throw new IOException("Metadata fail: " + resp.code());
                    }
                }
            } catch (Exception e) {
                scheduler.schedule(this::startAuto, 30, TimeUnit.SECONDS);
            }
        });
    }

    public void startConsume(int partition, long initialOffset) {
        scheduler.execute(() -> runConsumeLoop(partition, initialOffset));
    }

    private void runConsumeLoop(int partition, long defaultOffset) {
        long currentOffset = defaultOffset;
        if (config.isEnableExternalStore() && config.getOffsetStore() != null) {
            currentOffset = config.getOffsetStore().load(config.getTopic(), partition, defaultOffset);
        }

        // 关键修改：增加 clientId 参数
        HttpUrl httpUrl = HttpUrl.parse(config.getServerUrl() + "/stream").newBuilder()
                .addQueryParameter("topic", config.getTopic())
                .addQueryParameter("partition", String.valueOf(partition))
                .addQueryParameter("offset", String.valueOf(currentOffset))
                .addQueryParameter("mode", config.getMode().name())
                .addQueryParameter("clientId", config.getClientId()) 
                .build();

        Request request = new Request.Builder()
                .url(httpUrl)
                .header("Accept", "text/event-stream")
                .build();

        long finalOffset = currentOffset;
        EventSources.createFactory(client).newEventSource(request, new EventSourceListener() {
            private long lastSeenOffset = finalOffset;
            private final AtomicBoolean isFinished = new AtomicBoolean(false);

            @Override
            public void onEvent(@NotNull EventSource s, @Nullable String id, @Nullable String type, @NotNull String data) {
                if (isFinished.get()) return;

                if ("sleep".equals(type)) {
                    // data 现在返回的是占用该分区的 clientId
                    System.err.println("P" + partition + " locked by " + data + ". Sleeping...");
                    s.cancel();
                    isFinished.set(true);
                    scheduler.schedule(() -> runConsumeLoop(partition, lastSeenOffset), config.getSleepIntervalMin(), TimeUnit.MINUTES);
                } else if ("complete".equals(type)) {
                    s.cancel();
                    isFinished.set(true);
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
                if (isFinished.get()) return;
                s.cancel();
                isFinished.set(true);
                scheduler.schedule(() -> runConsumeLoop(partition, lastSeenOffset), config.getRetryIntervalSec(), TimeUnit.SECONDS);
            }
        });
    }

    public void shutdown() {
        scheduler.shutdown();
        client.dispatcher().executorService().shutdown();
    }
}
