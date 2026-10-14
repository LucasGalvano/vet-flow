"""Bot Python do vet_flow (Partes 1, 2 e relógio lógico da Parte 3)."""

import os
import random
import re
import sys
import threading
import time

import msgpack
import zmq

SERVER_ADDRESS = os.environ.get("SERVER_ADDRESS", "tcp://localhost:5555")
BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS", "tcp://localhost:5558")
BOT_NAME = os.environ.get("BOT_NAME", "bot-python-1")
ACTION = os.environ.get("ACTION", "LOGIN").upper()
CHANNEL_NAME = os.environ.get("CHANNEL_NAME", "avisos-gerais")
CHANNELS = [name.strip() for name in os.environ.get("CHANNELS", CHANNEL_NAME).split(",") if name.strip()]
MESSAGE = os.environ.get("MESSAGE", "mensagem de teste")
SUBSCRIBE_SECONDS = float(os.environ.get("SUBSCRIBE_SECONDS", "5"))
RECV_TIMEOUT_MS = int(os.environ.get("RECV_TIMEOUT_MS", "5000"))
BOT_DELAY_SECONDS = float(os.environ.get("BOT_DELAY_SECONDS", "1"))

logical_clock = 0
clock_lock = threading.Lock()


def observe_message(envelope: dict) -> None:
    global logical_clock
    received = envelope.get("logical_clock", 0)
    if isinstance(received, int):
        with clock_lock:
            logical_clock = max(logical_clock, received)


def build_envelope(msg_type: str, payload: dict) -> dict:
    global logical_clock
    with clock_lock:
        logical_clock += 1
        clock = logical_clock
    return {
        "type": msg_type,
        "sender_id": BOT_NAME,
        "sender_lang": "python",
        "timestamp": int(time.time() * 1000),
        "logical_clock": clock,
        "payload": payload,
    }


def request(socket, msg_type: str, payload: dict) -> dict:
    envelope = build_envelope(msg_type, payload)
    socket.send(msgpack.packb(envelope, use_bin_type=True))
    print(f"[SEND] {envelope}")
    response = msgpack.unpackb(socket.recv(), raw=False)
    observe_message(response)
    print(f"[RECV] {response}")
    return response


def make_req(context):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(SERVER_ADDRESS)
    return socket


def payload_ok(response):
    return (response.get("payload") or {}).get("status") == "OK"


def channel_list(socket):
    response = request(socket, "CHANNEL_LIST_REQUEST", {})
    return (response.get("payload") or {}).get("channels", [])


def receive_loop(context, channels, stop):
    # O socket é criado e usado pela mesma thread, como exige o ZeroMQ.
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVTIMEO, 300)
    sub.connect(BROKER_ADDRESS)
    for channel in channels:
        sub.setsockopt(zmq.SUBSCRIBE, channel.encode("utf-8"))
    while not stop.is_set():
        try:
            topic, body = sub.recv_multipart()
            envelope = msgpack.unpackb(body, raw=False)
            observe_message(envelope)
            received_at = int(time.time() * 1000)
            print(f"[PUBSUB-RECV] canal={topic.decode('utf-8')} "
                  f"mensagem={envelope.get('payload', {}).get('message')} "
                  f"timestamp_envio={envelope.get('timestamp')} "
                  f"timestamp_recebimento={received_at} "
                  f"logical_clock={envelope.get('logical_clock')}")
        except zmq.error.Again:
            continue
    sub.close()


def run_subscribe(context, channels, duration=None):
    if duration is None:
        stop = threading.Event()
        thread = threading.Thread(target=receive_loop, args=(context, channels, stop), daemon=True)
        thread.start()
        print(f"[CLIENT] '{BOT_NAME}' inscrito nos canais {channels} via {BROKER_ADDRESS}")
        return stop, thread

    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVTIMEO, 300)
    sub.connect(BROKER_ADDRESS)
    for channel in channels:
        sub.setsockopt(zmq.SUBSCRIBE, channel.encode("utf-8"))
    print(f"[CLIENT] '{BOT_NAME}' inscrito nos canais {channels} via {BROKER_ADDRESS}")
    deadline = time.time() + duration
    received = 0
    while time.time() < deadline:
        try:
            topic, body = sub.recv_multipart()
            envelope = msgpack.unpackb(body, raw=False)
            observe_message(envelope)
            received_at = int(time.time() * 1000)
            print(f"[PUBSUB-RECV] canal={topic.decode('utf-8')} "
                  f"mensagem={envelope.get('payload', {}).get('message')} "
                  f"timestamp_envio={envelope.get('timestamp')} "
                  f"timestamp_recebimento={received_at}")
            received += 1
        except zmq.error.Again:
            pass
    sub.close()
    print(f"[CLIENT] total de mensagens recebidas: {received}")


def run_bot_loop(context):
    req = make_req(context)
    login = request(req, "LOGIN_REQUEST", {"bot_name": BOT_NAME})
    if not payload_ok(login):
        raise RuntimeError("login recusado")

    channels = channel_list(req)
    base = re.sub(r"[^a-zA-Z0-9_-]", "-", BOT_NAME).lower()[:30]
    attempt = 0
    while len(channels) < 5 and attempt < 20:
        name = f"canal-{base}-{attempt + 1}"
        response = request(req, "CHANNEL_CREATE_REQUEST", {
            "channel_name": name, "bot_name": BOT_NAME,
        })
        if not payload_ok(response):
            print(f"[CLIENT] canal não criado: {(response.get('payload') or {}).get('error_msg')}")
        channels = channel_list(req)
        attempt += 1
    if not channels:
        raise RuntimeError("servidor não possui canais para publicar")

    subscribed = random.sample(channels, min(3, len(channels)))
    stop, thread = run_subscribe(context, subscribed)
    try:
        while True:
            channel = random.choice(channels)
            for index in range(1, 11):
                message = f"{BOT_NAME}: mensagem {index} no canal {channel}"
                response = request(req, "PUBLISH_REQUEST", {
                    "channel_name": channel, "message": message,
                })
                if not payload_ok(response):
                    print(f"[CLIENT] publicação recusada: {(response.get('payload') or {}).get('error_msg')}")
                time.sleep(BOT_DELAY_SECONDS)
    finally:
        stop.set()
        thread.join(timeout=1)
        req.close()


def build_one_shot():
    if ACTION == "LOGIN":
        return "LOGIN_REQUEST", {"bot_name": BOT_NAME}
    if ACTION == "CHANNEL_CREATE":
        return "CHANNEL_CREATE_REQUEST", {"channel_name": CHANNEL_NAME, "bot_name": BOT_NAME}
    if ACTION == "CHANNEL_LIST":
        return "CHANNEL_LIST_REQUEST", {}
    if ACTION == "PUBLISH":
        return "PUBLISH_REQUEST", {"channel_name": CHANNEL_NAME, "message": MESSAGE}
    raise ValueError(f"ACTION desconhecida: {ACTION}")


def main():
    context = zmq.Context()
    try:
        if ACTION == "SUBSCRIBE":
            run_subscribe(context, CHANNELS, SUBSCRIBE_SECONDS)
            return
        if ACTION in ("BOT", "BOT_LOOP"):
            run_bot_loop(context)
            return
        socket = make_req(context)
        msg_type, payload = build_one_shot()
        response = request(socket, msg_type, payload)
        if msg_type == "CHANNEL_LIST_REQUEST":
            print(f"[CLIENT] Canais existentes: {(response.get('payload') or {}).get('channels', [])}")
        elif not payload_ok(response):
            print(f"[CLIENT] erro: {(response.get('payload') or {}).get('error_msg')}")
            sys.exit(1)
        socket.close()
    finally:
        context.term()


if __name__ == "__main__":
    main()
