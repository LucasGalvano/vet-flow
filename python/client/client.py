"""
Python Client (bot) - Parte 1 (passo 3)

Escopo DESTE passo:
    - Socket ZeroMQ REQ, conforme especificado no enunciado.
    - Envia UMA requisicao por execucao, definida por ACTION:
        ACTION=LOGIN           -> LOGIN_REQUEST
        ACTION=CHANNEL_CREATE  -> CHANNEL_CREATE_REQUEST
        ACTION=CHANNEL_LIST    -> CHANNEL_LIST_REQUEST
    - Exibe a resposta recebida.

O client eh um bot: nao ha nenhuma interacao manual. Nome do bot,
endereco do servidor e a acao a executar vem de variaveis de ambiente
(facilita configurar via Docker Compose futuramente, sem mudar codigo).

Ver contrato completo em protocol/PROTOCOL.md.
"""

import os
import sys
import time

import msgpack
import zmq

SERVER_ADDRESS = os.environ.get("SERVER_ADDRESS", "tcp://localhost:5555")
BOT_NAME = os.environ.get("BOT_NAME", "bot-python-1")
ACTION = os.environ.get("ACTION", "LOGIN").upper()
CHANNEL_NAME = os.environ.get("CHANNEL_NAME", "avisos-gerais")

# Timeout de recepcao: evita que o bot fique bloqueado para sempre caso o
# servidor nao responda. Importante porque o projeto nao pode depender de
# interacao manual (regra do enunciado) -- o bot precisa poder desistir
# sozinho e sinalizar erro via exit code.
RECV_TIMEOUT_MS = int(os.environ.get("RECV_TIMEOUT_MS", "5000"))


def build_envelope(msg_type: str, payload: dict) -> dict:
    return {
        "type": msg_type,
        "sender_id": BOT_NAME,
        "sender_lang": "python",
        "timestamp": int(time.time() * 1000),
        "payload": payload,
    }


def build_request() -> dict:
    """Monta o envelope de requisicao de acordo com ACTION."""
    if ACTION == "LOGIN":
        return build_envelope("LOGIN_REQUEST", {"bot_name": BOT_NAME})

    if ACTION == "CHANNEL_CREATE":
        return build_envelope(
            "CHANNEL_CREATE_REQUEST",
            {"channel_name": CHANNEL_NAME, "bot_name": BOT_NAME},
        )

    if ACTION == "CHANNEL_LIST":
        return build_envelope("CHANNEL_LIST_REQUEST", {})

    print(f"[CLIENT] ACTION desconhecida: '{ACTION}'")
    sys.exit(1)


def print_result(response: dict) -> None:
    """Interpreta a resposta de acordo com seu 'type' e imprime um resumo."""
    resp_type = response.get("type")
    payload = response.get("payload", {})

    if resp_type == "CHANNEL_LIST_RESPONSE":
        channels = payload.get("channels", [])
        print(f"[CLIENT] Canais existentes ({len(channels)}): {channels}")
        return

    # LOGIN_RESPONSE e CHANNEL_CREATE_RESPONSE seguem a mesma convencao
    # de status/error_msg (ver protocol/PROTOCOL.md).
    if payload.get("status") == "OK":
        print(f"[CLIENT] Acao '{ACTION}' concluida com sucesso.")
    else:
        error = payload.get("error_msg", "erro desconhecido")
        print(f"[CLIENT] Acao '{ACTION}' falhou: {error}")
        sys.exit(1)


def main():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(SERVER_ADDRESS)
    print(f"[CLIENT] '{BOT_NAME}' conectando em {SERVER_ADDRESS} (ACTION={ACTION})")

    request = build_request()
    socket.send(msgpack.packb(request, use_bin_type=True))
    print(f"[SEND] {request}")

    try:
        raw = socket.recv()
        response = msgpack.unpackb(raw, raw=False)
        print(f"[RECV] {response}")
        print_result(response)
    except zmq.error.Again:
        print("[CLIENT] Timeout: servidor nao respondeu dentro do prazo")
        sys.exit(1)
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()