import argparse
import threading
import time
import sys
from confluent_kafka import Producer, KafkaException

def run_auth_storm(worker_id, config, iterations):
    """
    每个 Worker 尝试创建 Producer 并触发认证
    """
    print(f"[Worker {worker_id}] 启动...")
    
    count = 0
    while iterations == -1 or count < iterations:
        try:
            # 1. 创建 Producer 实例
            p = Producer(config)
            
            # 2. 触发认证：请求集群元数据是强制触发 SASL 认证最快的方式
            # 即使认证失败，由于 Broker 的 5s 延迟，这个调用会阻塞直到超时或 Broker 返回
            p.list_topics(timeout=3.0) 
            
        except KafkaException as e:
            # 我们预期这里会捕获到认证失败或超时
            pass
        except Exception as e:
            pass
        
        count += 1
        # 频繁重连，不设置 sleep 或仅设置极小值以形成“风暴”
        time.sleep(0.1)

def main():
    parser = argparse.ArgumentParser(description="Kafka 认证风暴模拟器 (SASL_SSL)")
    parser.add_argument("--bootstrap", required=True, help="Kafka Bootstrap Server 地址 (例如: kafka:9093)")
    parser.add_argument("--ca-cert", required=True, help="CA 证书路径 (ca.crt)")
    parser.add_argument("--user", default="wrong_user", help="错误的用户名")
    parser.add_argument("--password", default="wrong_password", help="错误的密码")
    parser.add_argument("--concurrency", type=int, default=10, help="并发客户端线程数")
    parser.add_argument("--iter", type=int, default=-1, help="每个线程尝试次数 (-1 为无限)")
    
    args = parser.parse_args()

    # Kafka 客户端配置
    # 注意：我们故意将 socket.timeout.ms 设小（比如 3-4s），
    # 这样它会比 Broker 的 5s 延迟先超时，从而主动断开连接，制造 CLOSE-WAIT
    kafka_config = {
        'bootstrap.servers': args.bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-512', 
        'sasl.username': args.user,
        'sasl.password': args.password,
        'ssl.ca.location': args.ca_cert,
        'socket.timeout.ms': 3000,      # 关键点：小于 Broker 的 5000ms 延迟
        'request.timeout.ms': 3000,
        'reconnect.backoff.ms': 100,
        'reconnect.backoff.max.ms': 1000,
    }

    print(f"--- 模拟开始 ---")
    print(f"目标集群: {args.bootstrap}")
    print(f"并发线程: {args.concurrency}")
    print(f"提示: 请观察 Broker 端 ss -lnt 状态")

    threads = []
    for i in range(args.concurrency):
        t = threading.Thread(target=run_auth_storm, args=(i, kafka_config, args.iter))
        t.daemon = True
        threads.append(t)
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n正在停止模拟...")

if __name__ == "__main__":
    main()
