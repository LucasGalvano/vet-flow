"""
Python Server - Parte 1 (passo 3)
Sistema distribuido de mensagens - dominio: clinica veterinaria

Escopo DESTE passo (nao adiantar nada alem disto):
    - Socket ZeroMQ REP, conforme especificado no enunciado para a Parte 1.
    - LOGIN_REQUEST / LOGIN_RESPONSE, com persistencia (passo anterior).
    - CHANNEL_CREATE_REQUEST / CHANNEL_CREATE_RESPONSE, com persistencia.
    - CHANNEL_LIST_REQUEST / CHANNEL_LIST_RESPONSE.

Ver contrato completo em protocol/PROTOCOL.md.
"""

import os
import time

import msgpack
import zmq

import persistence

BIND_ADDRESS = os.environ.get("SERVER_BIND_ADDRESS", "tcp://*:5555")

# Permite sobrescrever os caminhos de persistencia (util para testes
# isolados e, futuramente, para apontar para um volume montado no
# Docker Compose).
LOGINS_PATH = os.environ.get(
    "LOGINS_PERSISTENCE_PATH", persistence.DEFAULT_LOGINS_PATH
)
CHANNELS_PATH = os.environ.get(
    "CHANNELS_PERSISTENCE_PATH", persistence.DEFAULT_CHANNELS_PATH
)

# Estado em memoria, carregado do disco ao iniciar. Seguro sem lock
# porque o loop principal (main) processa uma requisicao REP por vez,
# sequencialmente -- ver nota de concorrencia em persistence.py.
known_logins = []
known_channels = []


def build_envelope(msg_type: str, payload: dict) -> dict:
    """Monta um envelope no formato do protocolo comum (ver PROTOCOL.md)."""
    return {
        "type": msg_type,
        "sender_id": "python-server",
        "sender_lang": "python",
        "timestamp": int(time.time() * 1000),  # epoch millis, UTC
        "payload": payload,
    }


def handle_login_request(envelope: dict) -> dict:
    """Trata LOGIN_REQUEST e devolve o envelope de LOGIN_RESPONSE."""
    bot_name = envelope.get("payload", {}).get("bot_name")

    if not bot_name:
        return build_envelope(
            "LOGIN_RESPONSE",
            {"status": "ERROR", "error_msg": "campo 'bot_name' ausente ou vazio"},
        )

    record = {
        "bot_name": bot_name,
        "sender_lang": envelope.get("sender_lang"),
        "timestamp": envelope.get("timestamp"),
    }

    global known_logins
    known_logins = persistence.append_login(record, LOGINS_PATH)

    print(
        f"[LOGIN] bot='{bot_name}' lang={envelope.get('sender_lang')} "
        f"timestamp={envelope.get('timestamp')} "
        f"(total de logins persistidos: {len(known_logins)})"
    )

    return build_envelope("LOGIN_RESPONSE", {"status": "OK"})


def handle_channel_create_request(envelope: dict) -> dict:
    """Trata CHANNEL_CREATE_REQUEST e devolve CHANNEL_CREATE_RESPONSE."""
    payload = envelope.get("payload", {})
    channel_name = payload.get("channel_name")
    bot_name = payload.get("bot_name")

    if not channel_name:
        return build_envelope(
            "CHANNEL_CREATE_RESPONSE",
            {"status": "ERROR", "error_msg": "campo 'channel_name' ausente ou vazio"},
        )

    if not bot_name:
        return build_envelope(
            "CHANNEL_CREATE_RESPONSE",
            {"status": "ERROR", "error_msg": "campo 'bot_name' ausente ou vazio"},
        )

    # Nome duplicado e tratado como erro (comparacao exata, case-sensitive).
    # [DECISAO DE IMPLEMENTACAO] -- o enunciado nao especifica esse caso.
    global known_channels
    already_exists = any(
        c.get("channel_name") == channel_name for c in known_channels
    )
    if already_exists:
        return build_envelope(
            "CHANNEL_CREATE_RESPONSE",
            {"status": "ERROR", "error_msg": f"canal '{channel_name}' ja existe"},
        )

    record = {
        "channel_name": channel_name,
        "created_by": bot_name,
        "timestamp": envelope.get("timestamp"),
    }
    known_channels = persistence.append_channel(record, CHANNELS_PATH)

    print(
        f"[CHANNEL_CREATE] channel='{channel_name}' created_by='{bot_name}' "
        f"(total de canais: {len(known_channels)})"
    )

    return build_envelope("CHANNEL_CREATE_RESPONSE", {"status": "OK"})


def handle_channel_list_request(envelope: dict) -> dict:
    """Trata CHANNEL_LIST_REQUEST e devolve CHANNEL_LIST_RESPONSE."""
    channel_names = [c.get("channel_name") for c in known_channels]
    return build_envelope("CHANNEL_LIST_RESPONSE", {"channels": channel_names})


def dispatch(envelope: dict) -> dict:
    """Roteia o envelope recebido para o handler correto pelo campo 'type'."""
    msg_type = envelope.get("type")

    if msg_type == "LOGIN_REQUEST":
        return handle_login_request(envelope)

    if msg_type == "CHANNEL_CREATE_REQUEST":
        return handle_channel_create_request(envelope)

    if msg_type == "CHANNEL_LIST_REQUEST":
        return handle_channel_list_request(envelope)

    # Tipo desconhecido nao deve derrubar o servidor (REP exige sempre 1
    # send() por recv(), senao o socket trava em estado inconsistente).
    return build_envelope(
        "ERROR_RESPONSE",
        {"status": "ERROR", "error_msg": f"tipo de mensagem desconhecido: {msg_type}"},
    )


def main():
    global known_logins, known_channels
    known_logins = persistence.load_logins(LOGINS_PATH)
    known_channels = persistence.load_channels(CHANNELS_PATH)
    print(
        f"[SERVER] Historico carregado: {len(known_logins)} login(s) e "
        f"{len(known_channels)} canal(is) previamente persistido(s)"
    )

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(BIND_ADDRESS)
    print(f"[SERVER] Python Server (REP) ouvindo em {BIND_ADDRESS}")

    try:
        while True:
            raw = socket.recv()

            # Cada mensagem e tratada isoladamente: qualquer falha ao
            # decodificar o MessagePack, ou ao processar o envelope
            # (campos ausentes, tipos inesperados, envelope que nao e um
            # dict, etc.), e capturada aqui e vira um ERROR_RESPONSE, em
            # vez de propagar e derrubar o servidor inteiro. O socket REP
            # exige exatamente 1 send() por recv(), entao SEMPRE
            # respondemos algo, mesmo em erro.
            try:
                envelope = msgpack.unpackb(raw, raw=False)
                print(f"[RECV] {envelope}")
                response = dispatch(envelope)
            except Exception as e:
                print(f"[ERROR] Falha ao processar mensagem: {e}")
                response = build_envelope(
                    "ERROR_RESPONSE",
                    {
                        "status": "ERROR",
                        "error_msg": f"mensagem invalida ou mal-formada: {e}",
                    },
                )

            socket.send(msgpack.packb(response, use_bin_type=True))
            print(f"[SEND] {response}")
    except KeyboardInterrupt:
        print("\n[SERVER] Encerrando...")
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()