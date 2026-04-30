package com.example.client;

import okhttp3.*;
import okhttp3.sse.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 修复版 Kafka SSE 客户端:
 * 引入 finished 标志位，精准区分“业务完成”与“网络异常”。
 */
public class KafkaOkHttpSseClient {
    private final String url = "http://localhost:8080/api/v1/kafka/stream";
    private final String topic = "test-topic";
    private final int limit = 5, MAX_RETRY = 10;
    
    private final Map<Integer, Long> offsets = new ConcurrentHashMap<>();
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicInteger retries = new AtomicInteger(0);
    private final AtomicBoolean finished = new AtomicBoolean(false);
    
    private final OkHttpClient client = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public void start() {
        if (finished.get()) return;
        try {
            String lastId = offsets.entrySet().stream()
                    .map(e -> e.getKey() + ":" + e.getValue())
                    .collect(Collectors.joining(","));

            HttpUrl httpUrl = HttpUrl.parse(url).newBuilder()
                    .addQueryParameter("topic", topic)
                    .addQueryParameter("limit", String.valueOf(limit))
                    .build();

            Request request = new Request.Builder()
                    .url(httpUrl)
                    .header("Accept", "text/event-stream")
                    .header("Last-Event-ID", lastId)
                    .build();

            EventSources.createFactory(client).newEventSource(request, listener);
        } catch (Exception e) {
            handleRetry(e.getMessage());
        }
    }

    private final EventSourceListener listener = new EventSourceListener() {
        @Override
        public void onEvent(EventSource s, String id, String type, String data) {
            if (finished.get()) return;

            if ("complete".equals(type)) { 
                System.out.println("Server signal: Finished."); 
                close(s);
                return; 
            }
            
            try {
                int p = Integer.parseInt(data.split("\"partition\":")[1].split(",")[0].trim());
                long o = Long.parseLong(data.split("\"offset\":")[1].split(",")[0].trim());
                
                System.out.println("Msg -> P:" + p + " O:" + o);
                offsets.put(p, o + 1);
                retries.set(0); 
                
                if (limit != -1 && count.incrementAndGet() >= limit) {
                    System.out.println("Limit reached.");
                    close(s);
                }
            } catch (Exception e) { System.err.println("Parse Error: " + data); }
        }

        @Override
        public void onFailure(EventSource s, Throwable t, Response r) {
            if (finished.get()) return; // 核心修复：如果是已完成状态，直接忽略任何错误回调

            if (r != null && r.code() >= 400 && r.code() < 500) {
                System.err.println("Fatal: " + r.code());
                close(s);
                return;
            }
            handleRetry(t != null ? t.getMessage() : "Unknown");
        }
    };

    private void handleRetry(String reason) {
        if (finished.get()) return;
        if (retries.incrementAndGet() <= MAX_RETRY) {
            System.err.println("Issue: " + reason + ". Retry " + retries.get() + "/" + MAX_RETRY);
            scheduler.schedule(this::start, 3, TimeUnit.SECONDS);
        } else {
            System.err.println("Max retries exceeded.");
            close(null);
        }
    }

    private void close(EventSource s) {
        if (finished.compareAndSet(false, true)) {
            if (s != null) s.cancel();
            scheduler.shutdown();
            client.dispatcher().executorService().shutdown();
        }
    }

    public static void main(String[] args) {
        new KafkaOkHttpSseClient().start();
    }
}
