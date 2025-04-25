# chat_ui.py
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Input, Static
from textual.containers import Vertical
from kafka import KafkaProducer, KafkaConsumer
import threading
import json

BOOTSTRAP_SERVERS = "3.36.114.98:9092"
TOPIC = "chat-topic"
USERNAME = input("이름을 입력하세요: ")

class ChatBox(Static):
    def on_mount(self):
        self.messages = []
        threading.Thread(target=self.listen_messages, daemon=True).start()

    def listen_messages(self):
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=f"{USERNAME}-group",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        for msg in consumer:
            data = msg.value
            if data["sender"] != USERNAME:
                self.messages.append(f"{data['sender']}: {data['message']}")
                self.update("\n".join(self.messages))

class ChatApp(App):
    CSS_PATH = None

    def compose(self) -> ComposeResult:
        yield Header()
        self.chat_box = ChatBox()
        yield Vertical(self.chat_box)
        self.input = Input(placeholder="메시지를 입력하세요...")
        yield self.input
        yield Footer()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        message = event.value.strip()
        if message:
            self.chat_box.messages.append(f"나: {message}")
            self.chat_box.update("\n".join(self.chat_box.messages))
            self.send_kafka_message(message)
            self.input.value = ""

    def send_kafka_message(self, msg):
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        )
        producer.send(TOPIC, {"sender": USERNAME, "message": msg})
        producer.flush()
        producer.close()

if __name__ == "__main__":
    ChatApp().run()

