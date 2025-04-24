#cli.py
from kafka import KafkaProducer
import typer
import json

def chatpro():
    bootstrap_servers = input("🛠️  Kafka bootstrap_servers 주소 입력 (예: 3.36.114.98:9093): ").strip()
    topic = input("🧭 전송할 토픽 이름 입력: ").strip()
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    print(f"📡 Kafka 연결됨: {bootstrap_servers}")
    print(f"✉️  메시지 전송 대상 토픽: {topic}")

    try:
        while True:
            message = input(">>> ")
            if message.strip():
                producer.send(topic, message)
                producer.flush()
                print("✅ 메시지 전송 완료")
    except KeyboardInterrupt:
        print("\n👋 종료합니다.")
    finally:
        producer.close()

if __name__ == "__main__":
    chatpro()

