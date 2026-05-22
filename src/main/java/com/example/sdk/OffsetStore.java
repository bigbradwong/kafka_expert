package com.example.sdk;

import redis.clients.jedis.Jedis;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 分布式/本地位移存储接口 (JDK 1.8 兼容)
 */
public interface OffsetStore {
    void save(String topic, int partition, long offset);
    long load(String topic, int partition, long defaultOffset);
}

/**
 * 本地文件实现 (JDK 1.8)
 */
class LocalFileOffsetStore implements OffsetStore {
    private final File directory;

    public LocalFileOffsetStore(String path) {
        this.directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    @Override
    public void save(String topic, int partition, long offset) {
        File file = new File(directory, topic + "-" + partition + ".off");
        try {
            byte[] bytes = String.valueOf(offset).getBytes(StandardCharsets.UTF_8);
            Files.write(file.toPath(), bytes);
        } catch (IOException e) {
            System.err.println("Failed to save offset to file: " + file.getAbsolutePath());
        }
    }

    @Override
    public long load(String topic, int partition, long defaultOffset) {
        File file = new File(directory, topic + "-" + partition + ".off");
        if (!file.exists()) return defaultOffset;
        try {
            byte[] bytes = Files.readAllBytes(file.toPath());
            String content = new String(bytes, StandardCharsets.UTF_8);
            return Long.parseLong(content.trim());
        } catch (Exception e) {
            return defaultOffset;
        }
    }
}

/**
 * JDBC 实现 (JDK 1.8)
 */
class JdbcOffsetStore implements OffsetStore {
    private final DataSource dataSource;
    private final String tableName = "kafka_sse_offsets";

    public JdbcOffsetStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(String topic, int partition, long offset) {
        String sql = "INSERT INTO " + tableName + " (topic, part, off) VALUES (?, ?, ?) " +
                     "ON DUPLICATE KEY UPDATE off = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setLong(3, offset);
            ps.setLong(4, offset);
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public long load(String topic, int partition, long defaultOffset) {
        String sql = "SELECT off FROM " + tableName + " WHERE topic = ? AND part = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong("off");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return defaultOffset;
    }
}

/**
 * Redis 实现 (JDK 1.8)
 */
class RedisOffsetStore implements OffsetStore {
    private final Jedis jedis;
    private final String prefix = "kafka:sse:offset:";

    public RedisOffsetStore(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public void save(String topic, int partition, long offset) {
        jedis.set(prefix + topic + ":" + partition, String.valueOf(offset));
    }

    @Override
    public long load(String topic, int partition, long defaultOffset) {
        String val = jedis.get(prefix + topic + ":" + partition);
        return val != null ? Long.parseLong(val) : defaultOffset;
    }
}

/**
 * S3 实现 (JDK 1.8)
 */
class S3OffsetStore implements OffsetStore {
    private final S3Client s3;
    private final String bucket;

    public S3OffsetStore(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public void save(String topic, int partition, long offset) {
        String key = "offsets/" + topic + "/" + partition + ".txt";
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromString(String.valueOf(offset)));
    }

    @Override
    public long load(String topic, int partition, long defaultOffset) {
        String key = "offsets/" + topic + "/" + partition + ".txt";
        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucket).key(key).build());
            return Long.parseLong(new String(objectBytes.asByteArray(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            return defaultOffset;
        }
    }
}
