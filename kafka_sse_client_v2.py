import requests
import time
import json

def stream_kafka_messages():
    """
    Python SSE 客户端 V2: 具备工业级重连和断点续传能力
    """
    url = "http://localhost:8080/api/v1/kafka/stream"
    topic = "test-topic"
    limit = 1000
    
    # 核心状态：Map<Partition, Offset>
    last_seen_offsets = {} 
    total_processed = 0

    print(f"Python Client V2 started. Target: {limit} messages.")

    while limit == -1 or total_processed < limit:
        try:
            # 构造断点续传 Header: "0:100,1:250"
            last_event_id = ",".join([f"{p}:{o}" for p, o in last_seen_offsets.items()])
            
            headers = {'Accept': 'text/event-stream'}
            if last_event_id:
                headers['Last-Event-ID'] = last_event_id
                print(f">>> Resuming stream from offsets: {last_event_id}")

            params = {
                "topic": topic,
                "limit": limit
            }

            # 使用 stream=True 进行长连接拉取
            with requests.get(url, params=params, headers=headers, stream=True, timeout=60) as r:
                if r.status_code != 200:
                    print(f"Server error {r.status_code}. Retrying in 3s...")
                    time.sleep(3)
                    continue

                for line in r.iter_lines():
                    if line:
                        decoded_line = line.decode('utf-8')
                        
                        if decoded_line.startswith("data:"):
                            raw_data = decoded_line[5:].strip()
                            try:
                                msg = json.loads(raw_data)
                                p, o = msg['partition'], msg['offset']
                                
                                # 业务逻辑处理
                                print(f"Received -> Partition:{p}, Offset:{o}")
                                
                                # 更新本地状态，以便断开后能精准续传
                                last_seen_offsets[p] = o + 1
                                total_processed += 1
                                
                                if limit != -1 and total_processed >= limit:
                                    print("Reached target limit. Finished.")
                                    return
                            except Exception as parse_err:
                                print(f"Parse error: {parse_err}")
            
        except (requests.exceptions.RequestException, Exception) as e:
            print(f"Connection lost: {e}. Reconnecting in 3 seconds...")
            time.sleep(3)

if __name__ == "__main__":
    stream_kafka_messages()
