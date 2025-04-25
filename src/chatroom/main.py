import threading
import time
from kafka import KafkaConsumer, KafkaProducer
import json

def consume_loop(broker, topic, my_name):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id="chatroom-heejin"
    )

    for message in consumer:
        try:
            decoded = json.loads(message.value.decode('utf-8'))
        except json.JSONDecodeError:
            continue

        sender = decoded.get("sender")
        msg = decoded.get("message")

        if sender == my_name:
            continue  # 자기 메시지는 무시

        print(f"\n🧑‍💬 {sender} > {msg}\n>>> ", end="", flush=True)

def main():
    broker = input("Kafka broker 주소 (예: 34.64.x.x:9093): ")
    topic = input("채팅 토픽 이름 (예: chatroom): ")
    my_name = input("내 이름 (식별자): ")

    # 백그라운드 Consumer 쓰레드 실행
    thread = threading.Thread(
        target=consume_loop,
        args=(broker, topic, my_name),
        daemon=True
    )
    thread.start()

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

    print("✉️ 채팅 시작! 'exit' 입력 시 종료\n")

    try:
        while True:
            msg = input(">>> ")
            if msg.strip().lower() == "exit":
                producer.send(topic, {"sender": my_name, "message": "exit"})
                producer.flush()
                break
            producer.send(topic, {"sender": my_name, "message": msg})
            producer.flush()
    except KeyboardInterrupt:
        print("\n🛑 강제 종료")
    finally:
        producer.close()

# ✅ 스크립트 실행용 entry_point 함수
def entry_point():
    main()

if __name__ == "__main__":
    entry_point()
