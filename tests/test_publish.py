"""
Teste do contrato de PUBLISH_REQUEST (ver protocol/PROTOCOL.md), valido
para QUALQUER servidor (Python ou Java): fala so por sockets ZeroMQ.

Para cada caso verifica:
  1. a resposta do servidor (status e texto do erro);
  2. que mensagens rejeitadas NAO foram publicadas (um subscriber real,
     ligado ao broker, escuta o canal durante todo o teste);
  3. que mensagens rejeitadas NAO foram persistidas (se --messages-file
     for informado, compara o numero de registros antes/depois).

Uso:
  python tests/test_publish_validation.py \
      --server tcp://localhost:5555 --broker tcp://localhost:5558 \
      --messages-file python/server/data/messages.msgpack

Pre-requisitos: broker e UM servidor rodando. Sai com codigo 1 se algum
caso falhar. Nao e preciso criar o canal antes (o script cria).
"""
import argparse
import os
import sys
import threading
import time

import msgpack
import zmq

CHANNEL = "teste-contrato"
OK = "OK"


def build(msg_type, payload):
    return {
        "type": msg_type,
        "sender_id": "test-contract",
        "sender_lang": "python",
        "timestamp": int(time.time() * 1000),
        "payload": payload,
    }


def request(ctx, server, envelope):
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 5000)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(server)
    try:
        s.send(msgpack.packb(envelope, use_bin_type=True))
        return msgpack.unpackb(s.recv(), raw=False)
    finally:
        s.close()


def count_records(path):
    if not path or not os.path.exists(path):
        return 0
    raw = open(path, "rb").read()
    return len(msgpack.unpackb(raw, raw=False)) if raw else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="tcp://localhost:5555")
    ap.add_argument("--broker", default="tcp://localhost:5558")
    ap.add_argument("--messages-file", default=None)
    args = ap.parse_args()

    ctx = zmq.Context()

    # Subscriber real escutando o canal durante todo o teste.
    received = []
    stop = threading.Event()

    def listen():
        sub = ctx.socket(zmq.SUB)
        sub.connect(args.broker)
        sub.setsockopt(zmq.SUBSCRIBE, CHANNEL.encode("utf-8"))
        sub.setsockopt(zmq.RCVTIMEO, 200)
        while not stop.is_set():
            try:
                topic, body = sub.recv_multipart()
                received.append(msgpack.unpackb(body, raw=False))
            except zmq.error.Again:
                continue
        sub.close()

    t = threading.Thread(target=listen, daemon=True)
    t.start()
    time.sleep(1.5)  # slow joiner: deixa a assinatura propagar

    # Garante o canal (ignora "ja existe").
    request(ctx, args.server, build("CHANNEL_CREATE_REQUEST",
            {"channel_name": CHANNEL, "bot_name": "test-contract"}))

    ABSENT = object()  # marca "campo nao enviado"
    nome_erro_vazio = "campo 'message' ausente ou vazio"
    nome_erro_tipo = "campo 'message' deve ser uma string"

    # (rotulo, valor de message, (status, erro) esperados)
    cases = [
        ("string valida",            "mensagem valida 1",  (OK, None)),
        ("numero (456)",             456,                  ("ERROR", nome_erro_tipo)),
        ("numero zero (0)",          0,                    ("ERROR", nome_erro_tipo)),
        ("numero float (1.5)",       1.5,                  ("ERROR", nome_erro_tipo)),
        ("booleano true",            True,                 ("ERROR", nome_erro_tipo)),
        ("booleano false",           False,                ("ERROR", nome_erro_tipo)),
        ("lista [\"a\"]",            ["a"],                ("ERROR", nome_erro_tipo)),
        ("lista vazia []",           [],                   ("ERROR", nome_erro_tipo)),
        ("objeto {\"a\":1}",         {"a": 1},             ("ERROR", nome_erro_tipo)),
        ("objeto vazio {}",          {},                   ("ERROR", nome_erro_tipo)),
        ("null",                     None,                 ("ERROR", nome_erro_vazio)),
        ("campo ausente",            ABSENT,               ("ERROR", nome_erro_vazio)),
        ("string vazia \"\"",        "",                   ("ERROR", nome_erro_vazio)),
        ("string valida (servidor vivo?)", "mensagem valida 2", (OK, None)),
    ]
    extra = [
        ("channel_name numerico",
         {"channel_name": 123, "message": "x"},
         ("ERROR", "campo 'channel_name' deve ser uma string")),
        ("channel_name lista",
         {"channel_name": [CHANNEL], "message": "x"},
         ("ERROR", "campo 'channel_name' deve ser uma string")),
        ("channel_name null",
         {"channel_name": None, "message": "x"},
         ("ERROR", "campo 'channel_name' ausente ou vazio")),
    ]

    before = count_records(args.messages_file)
    failures = 0
    expected_valid = 0

    def check(label, payload, expected):
        nonlocal failures, expected_valid
        r = request(ctx, args.server, build("PUBLISH_REQUEST", payload))
        p = r.get("payload") or {}
        got = (p.get("status"), p.get("error_msg"))
        ok = got == expected and r.get("type") == "PUBLISH_RESPONSE"
        if expected[0] == OK:
            expected_valid += 1
        print(f"[{'PASS' if ok else 'FAIL'}] {label:<34} -> {got[0]} {got[1] or ''}")
        if not ok:
            print(f"        esperado: {expected}  (type={r.get('type')})")
            failures += 1

    for label, value, expected in cases:
        payload = {"channel_name": CHANNEL}
        if value is not ABSENT:
            payload["message"] = value
        check(f"message: {label}", payload, expected)
    for label, payload, expected in extra:
        check(label, payload, expected)

    time.sleep(1.5)  # deixa chegar o que tiver sido publicado
    stop.set()
    t.join()

    # --- Nada rejeitado pode ter sido publicado ---
    texts = [e.get("payload", {}).get("message") for e in received]
    all_str = all(isinstance(m, str) for m in texts)
    pub_ok = all_str and sorted(texts) == ["mensagem valida 1", "mensagem valida 2"]
    print(f"[{'PASS' if pub_ok else 'FAIL'}] publicado so o que era valido  -> recebido: {texts}")
    failures += 0 if pub_ok else 1

    # --- Nada rejeitado pode ter sido persistido ---
    if args.messages_file:
        added = count_records(args.messages_file) - before
        per_ok = added == expected_valid
        print(f"[{'PASS' if per_ok else 'FAIL'}] persistido so o que era valido -> "
              f"+{added} registro(s), esperado +{expected_valid}")
        failures += 0 if per_ok else 1
    else:
        print("[SKIP] persistencia nao verificada (use --messages-file)")

    print()
    print("RESULTADO:", "TODOS OS TESTES PASSARAM" if failures == 0 else f"{failures} FALHA(S)")
    ctx.term()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()