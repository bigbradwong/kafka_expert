#!/bin/bash
# migration-sync-v2.sh

# --- 配置区 ---
KAFKA_DATA_DIR="/mnt/data/kafka-logs"
ZK_DATA_DIR="/mnt/data/zookeeper-data"

NEW_NODE_IP="192.168.1.xxx" # 目标 Rocky 机器临时 IP
REMOTE_USER="root"
LOG_FILE="/tmp/migration_rsync.log"

# --- 逻辑区 ---
MODE=$1 # FULL or FINAL
if [[ "$MODE" != "FULL" && "$MODE" != "FINAL" ]]; then
    echo "Usage: $0 {FULL|FINAL}"
    exit 1
fi

sync_kafka() {
    echo "[$(date)] Syncing KAFKA ($MODE)..."
    if [ "$MODE" == "FULL" ]; then
        # 预同步：排除索引文件
        rsync -avzP --human-readable \
            --exclude '*.index' --exclude '*.timeindex' \
            "$KAFKA_DATA_DIR/" "$REMOTE_USER@$NEW_NODE_IP:$KAFKA_DATA_DIR/"
    else
        # 停机同步：全量一致性
        rsync -avzP --delete --human-readable \
            "$KAFKA_DATA_DIR/" "$REMOTE_USER@$NEW_NODE_IP:$KAFKA_DATA_DIR/"
    fi
}

sync_zookeeper() {
    echo "[$(date)] Syncing ZOOKEEPER ($MODE)..."
    if [ "$MODE" == "FULL" ]; then
        # 遵循建议：预同步排除 log.* 文件和 snapshot.*，只同步静态配置或空目录
        # 这样可以保证 FULL 模式绝对不会因为文件变化而报错
        rsync -avzP --human-readable \
            --exclude 'log.*' --exclude 'snapshot.*' --exclude 'myid' \
            "$ZK_DATA_DIR/" "$REMOTE_USER@$NEW_NODE_IP:$ZK_DATA_DIR/"
    else
        # 停机同步：ZK 数据通常很小，此时一次性同步 log 和 snapshot 速度非常快
        rsync -avzP --delete --human-readable \
            "$ZK_DATA_DIR/" "$REMOTE_USER@$NEW_NODE_IP:$ZK_DATA_DIR/"
    fi
}

# 执行同步
sync_kafka
sync_zookeeper

echo "[$(date)] $MODE migration sync completed successfully." | tee -a $LOG_FILE
