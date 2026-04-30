import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 具备断点续传能力的 Kafka SSE 客户端
 */
public class KafkaSseClient {
    
    // 核心状态：记录已处理的每个分区的 Offset
    private static final Map<Integer, Long> lastSeenOffsets = new ConcurrentHashMap<>();
    private static int totalProcessed = 0;

    public static void main(String[] args) throws InterruptedException {
        String serverUrl = "http://localhost:8080/api/v1/kafka/stream";
        String topic = "test-topic";
        int limit = 1000;

        System.out.println("Client started. Target messages: " + limit);

        while (limit == -1 || totalProcessed < limit) {
            try {
                connectAndStream(serverUrl, topic, limit);
            } catch (Exception e) {
                System.err.println("Connection interrupted. Reconnecting in 3s... Cause: " + e.getMessage());
                Thread.sleep(3000);
            }
        }
        System.out.println("Task completed. Total processed: " + totalProcessed);
    }

    private static void connectAndStream(String serverUrl, String topic, int limit) throws Exception {
        // 构造 Last-Event-ID 头信息 (断点续传的关键)
        String lastEventId = lastSeenOffsets.entrySet().stream()
                .map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(","));
        
        StringBuilder urlBuilder = new StringBuilder(serverUrl)
                .append("?topic=").append(URLEncoder.encode(topic, StandardCharsets.UTF_8))
                .append("&limit=").append(limit);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(urlBuilder.toString()))
                .header("Accept", "text/event-stream");

        if (!lastEventId.isEmpty()) {
            // 标准 SSE 协议头，服务端优先识别此头
            requestBuilder.header("Last-Event-ID", lastEventId);
            System.out.println(">>> Resuming stream from offsets: " + lastEventId);
        }

        HttpResponse<java.io.InputStream> response = client.send(requestBuilder.build(), 
                HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Server returned status code: " + response.statusCode());
        }

        try (Scanner scanner = new Scanner(response.body())) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.startsWith("data:")) {
                    String json = line.substring(5).trim();
                    handleMessage(json);
                }
            }
        }
    }

    private static void handleMessage(String json) {
        // 模拟解析 JSON (实际应用中请使用 Jackson/Gson)
        try {
            // 简单提取以解析: {"partition": 0, "offset": 123, ...}
            int p = Integer.parseInt(json.split("\"partition\":")[1].split(",")[0].trim());
            long o = Long.parseLong(json.split("\"offset\":")[1].split(",")[0].trim());
            
            // 业务处理逻辑
            System.out.println("Processing Msg -> Partition: " + p + ", Offset: " + o);
            
            // 处理成功后更新状态：下次请求从此 Offset + 1 开始
            lastSeenOffsets.put(p, o + 1);
            totalProcessed++;
        } catch (Exception e) {
            System.err.println("Failed to parse message: " + json);
        }
    }
}
