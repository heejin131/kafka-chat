from textual.app import App, ComposeResult
from textual.widgets import Log, Input, Button, Static
from textual.containers import Container
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json
import threading
import re
from asyncio import create_task
from threading import Timer
import os


class KafkaChatApp(App):
    BINDINGS = [("ctrl+c", "quit", "앱 종료"), ("escape", "quit", "앱 종료")]
    CSS_PATH = os.path.abspath("/home/gmlwls5168/code/kafka-chat/src/chatroom/chatroom.tcss")

    def __init__(self):
        super().__init__()
        self.broker = ""
        self.topic = ""
        self.my_name = ""
        self.consumer = None
        self.producer = None
        self.chat_log = None
        self._typing_timer = None
        self._last_typing = ""

    def compose(self) -> ComposeResult:
        yield Container(
            Static("💬 Kafka 채팅 시작", id="title"),
            Input(placeholder="📡 Kafka broker (예: 34.64.x.x:9093)", id="broker"),
            Input(placeholder="💬 채팅 토픽", id="topic"),
            Input(placeholder="🧑 내 이름", id="name"),
            Button("✅ 채팅 시작", id="start"),
            id="main-container"
        )

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "start":
            self.broker = self.query_one("#broker", Input).value.strip()
            self.topic = self.query_one("#topic", Input).value.strip()
            self.my_name = self.query_one("#name", Input).value.strip()

            self.query_one("#title").remove()
            self.query_one("#broker").remove()
            self.query_one("#topic").remove()
            self.query_one("#name").remove()
            event.button.remove()

            self.chat_log = Log(highlight=True, auto_scroll=True, id="chat-log")
            container = self.query_one("#main-container")
            container.mount_all([
                self.chat_log,
                Input(placeholder="메시지를 입력하세요... (exit 입력 시 종료)", id="chat-input"),
                Static("", id="typing-notice")
            ])

            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.broker,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=f"chat-{self.my_name}"
            )

            self.producer = KafkaProducer(
                bootstrap_servers=self.broker,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
            )

            threading.Thread(target=self.consume_loop, daemon=True).start()

    def consume_loop(self):
        for msg in self.consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
                sender = data.get("sender")
                msg_type = data.get("type")

                if sender == self.my_name:
                    continue

                if msg_type == "message":
                    message = data.get("message")
                    self.call_from_thread(lambda: self.append_message(self.format_message(sender, message)))
                    self.call_from_thread(self.clear_typing_notice)
                elif msg_type == "typing":
                    self.call_from_thread(lambda: self.show_typing(sender))
                elif msg_type == "exit":
                    self.call_from_thread(lambda: self.append_message(
                        self.format_message("시스템", f"❗ {sender} 님이 채팅을 종료했습니다.")
                    ))
            except Exception as e:
                self.call_from_thread(lambda: self.append_message(f"❌ JSON 오류: {e}"))

    def append_message(self, message: str):
        if self.chat_log:
            self.chat_log.write_line(message)

    async def on_input_submitted(self, event: Input.Submitted):
        msg = event.value.strip()
        if msg.lower() == "exit":
            self.append_message(self.format_message("시스템", "👋 채팅을 종료합니다."))
            self.producer.send(self.topic, {
                "sender": self.my_name,
                "type": "exit"
            })
            self.producer.flush()
            await self.action_quit()
            return

        self.append_message(self.format_message(self.my_name, msg))
        self.producer.send(self.topic, {
            "sender": self.my_name,
            "type": "message",
            "message": msg
        })
        self.producer.flush()
        event.input.value = ""

    async def on_input_changed(self, event: Input.Changed) -> None:
        if not hasattr(self, "_last_typing") or self._last_typing != event.value:
            self._last_typing = event.value
            create_task(self.send_typing_event())

    async def send_typing_event(self):
        if self.producer:
            self.producer.send(self.topic, {
                "sender": self.my_name,
                "type": "typing"
            })
            self.producer.flush()

    def show_typing(self, sender: str):
        self.query_one("#typing-notice", Static).update(f"✏️ {sender} 님이 입력 중...")
        if self._typing_timer:
            self._typing_timer.cancel()
        self._typing_timer = Timer(3, self.clear_typing_notice)
        self._typing_timer.start()

    def clear_typing_notice(self):
        self.query_one("#typing-notice", Static).update("")

    def timestamp(self) -> str:
        return datetime.now().strftime("%H:%M:%S")

    def strip_tags(self, text: str) -> str:
        return re.sub(r"\[[^\]]+\]", "", text)

    def format_message(self, sender: str, message: str) -> str:
        ts = self.timestamp()
        if sender == self.my_name:
            label = f"🟢 {sender}"
        elif sender == "시스템":
            label = f"🔴 {sender}"
        else:
            label = f"🔵 {sender}"
        line = f"{label} > {message}"
        padding = max(0, 60 - len(self.strip_tags(line)))
        return f"{line}{' ' * padding}[{ts}]"

    def on_exit(self):
        if self.producer:
            self.producer.close()


def entry_point():
    app = KafkaChatApp()
    app.run()


if __name__ == "__main__":
    entry_point()
