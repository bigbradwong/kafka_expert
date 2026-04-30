import requests

def stream_kafka_messages():
    # 配置参数
    url = "http://localhost:8080/api/v1/kafka/stream"
    params = {
        "topic": "test-topic",
        "offsets": "0:100,1:250", # 格式 "partition:offset,..."
        "limit": -1               # -1 使用服务端默认限制
    }
    
    print(f"Connecting to {url} with params {params}...")
    
    try:
        # stream=True 允许逐块读取
        with requests.get(url, params=params, stream=True, headers={'Accept': 'text/event-stream'}) as r:
            if r.status_code != 200:
                print(f"Error: {r.status_code} - {r.text}")
                return

            print("--- Connection Established ---")
            for line in r.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    
                    # 处理 ID 行 (分区-位移)
                    if decoded_line.startswith("id:"):
                        msg_id = decoded_line[3:].strip()
                        print(f"[MsgID: {msg_id}] ", end="")
                    
                    # 处理数据行 (JSON 字符串)
                    elif decoded_line.startswith("data:"):
                        json_data = decoded_line[5:].strip()
                        print(f"Received: {json_data}")
                    
                    # 忽略以 ':' 开头的心跳行
            
            print("--- Stream Completed by Server ---")

    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    stream_kafka_messages()
