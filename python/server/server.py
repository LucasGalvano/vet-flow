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
MESSAGES_PATH = os.environ.get(
    "MESSAGES_PERSISTENCE_PATH", persistence.DEFAULT_MESSAGES_PATH
)

# Endereco do XSUB do broker (Parte 2) -- o servidor conecta um socket
# PUB nele para publicar mensagens. Processo separado, ver broker/broker.py.
BROKER_XSUB_ADDRESS = os.environ.get("BROKER_XSUB_ADDRESS", "tcp://localhost:5557")
REFERENCE_ADDRESS = os.environ.get("REFERENCE_ADDRESS", "tcp://localhost:5559")
SERVER_NAME = os.environ.get("SERVER_NAME", "python-server")
HEARTBEAT_EVERY = int(os.environ.get("HEARTBEAT_EVERY", "10"))
# Resiliencia frente a um Reference Service indisponivel (ver CONTEXTO.md, D24):
#  - timeout de cada troca com o Reference (antes fixo em 5000 ms);
#  - tentativas explicitas de registro, com intervalo inicial e backoff
#    exponencial limitado (o intervalo dobra a cada falha ate o maximo).
REFERENCE_TIMEOUT_MS = max(1, int(os.environ.get("REFERENCE_TIMEOUT_MS", "1000")))
REFERENCE_RETRY_INITIAL_SECONDS = max(
    0.05, float(os.environ.get("REFERENCE_RETRY_INITIAL_SECONDS", "1")))
REFERENCE_RETRY_MAX_SECONDS = max(
    REFERENCE_RETRY_INITIAL_SECONDS,
    float(os.environ.get("REFERENCE_RETRY_MAX_SECONDS", "10")))
# O recv do REP "acorda" a cada IDLE_TICK_MS para o loop poder tentar o
# registro mesmo sem trafego de clients (sem threads).
IDLE_TICK_MS = 500

# Estado em memoria, carregado do disco ao iniciar. Seguro sem lock
# porque o loop principal (main) processa uma requisicao REP por vez,
# sequencialmente -- ver nota de concorrencia em persistence.py.
known_logins = []
known_channels = []

# Socket PUB global, criado em main() e usado pelo handler de PUBLISH_REQUEST.
pub_socket = None
reference_socket = None
logical_clock = 0
clock_offset_ms = 0
client_messages_received = 0
server_rank = None
zmq_context = None
reference_registered = False
registration_retry_delay = REFERENCE_RETRY_INITIAL_SECONDS
next_registration_attempt = 0.0


def physical_timestamp_ms() -> int:
    return int(time.time() * 1000) + clock_offset_ms


def observe_message(envelope: dict) -> None:
    global logical_clock
    value = envelope.get("logical_clock", 0)
    if isinstance(value, int):
        logical_clock = max(logical_clock, value)


def build_envelope(msg_type: str, payload: dict) -> dict:
    """Monta um envelope no formato do protocolo comum (ver PROTOCOL.md)."""
    global logical_clock
    logical_clock += 1
    return {
        "type": msg_type,
        "sender_id": "python-server",
        "sender_lang": "python",
        "timestamp": physical_timestamp_ms(),  # epoch millis, UTC sincronizado
        "logical_clock": logical_clock,
        "payload": payload,
    }


def new_reference_socket():
    sock = zmq_context.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, REFERENCE_TIMEOUT_MS)
    sock.setsockopt(zmq.SNDTIMEO, REFERENCE_TIMEOUT_MS)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(REFERENCE_ADDRESS)
    return sock


def reset_reference_socket() -> None:
    """Descarta e recria o socket REQ do Reference.

    Um REQ que estourou o timeout fica preso em "aguardando resposta": todo
    send seguinte falha com "Operation cannot be accomplished in current
    state" e ele nunca se recupera sozinho. Recriar o socket (LINGER 0 descarta
    a requisicao pendente) e a forma padrao de sair desse estado.
    """
    global reference_socket
    try:
        reference_socket.close()
    except Exception:
        pass
    reference_socket = new_reference_socket()


def reference_request(msg_type: str, payload: dict) -> dict:
    """Troca uma mensagem MessagePack com o Reference Service."""
    request = build_envelope(msg_type, payload)
    try:
        reference_socket.send(msgpack.packb(request, use_bin_type=True))
        raw_response = reference_socket.recv()
    except zmq.ZMQError:
        reset_reference_socket()
        raise
    response = msgpack.unpackb(raw_response, raw=False)
    observe_message(response)
    response_payload = response.get("payload") or {}
    reference_time = response_payload.get("reference_time_ms")
    if isinstance(reference_time, int):
        global clock_offset_ms
        clock_offset_ms = reference_time - int(time.time() * 1000)
    if response_payload.get("status") != "OK":
        raise RuntimeError(response_payload.get("error_msg", "Reference Service error"))
    return response_payload


def schedule_registration_retry(reason) -> None:
    """Marca o servidor como nao registrado e agenda a proxima tentativa."""
    global reference_registered, registration_retry_delay, next_registration_attempt
    reference_registered = False
    next_registration_attempt = time.monotonic() + registration_retry_delay
    print(f"[REFERENCE] indisponivel ({reason}); nova tentativa de registro "
          f"em {registration_retry_delay:g}s")
    registration_retry_delay = min(registration_retry_delay * 2,
                                   REFERENCE_RETRY_MAX_SECONDS)


def try_register() -> bool:
    """Tenta registrar no Reference. Nunca levanta excecao: em falha, agenda a
    proxima tentativa (backoff)."""
    global server_rank, reference_registered, registration_retry_delay
    try:
        registration = reference_request(
            "REFERENCE_REGISTER_REQUEST", {"server_name": SERVER_NAME}
        )
    except Exception as exc:
        schedule_registration_retry(exc)
        return False
    server_rank = registration.get("rank")
    reference_registered = True
    registration_retry_delay = REFERENCE_RETRY_INITIAL_SECONDS
    print(f"[SERVER] registrado como {SERVER_NAME}, rank={server_rank}, "
          f"hora ajustada em {clock_offset_ms}ms")
    return True


def maybe_register() -> None:
    """Chamado pelo loop principal: se nao ha registro e ja passou a hora da
    proxima tentativa, tenta registrar."""
    if not reference_registered and time.monotonic() >= next_registration_attempt:
        try_register()


def send_heartbeat() -> None:
    if not reference_registered:
        return  # sem registro, quem cuida disso e maybe_register()
    try:
        payload = reference_request(
            "REFERENCE_HEARTBEAT_REQUEST", {"server_name": SERVER_NAME}
        )
        print(f"[HEARTBEAT] servidor={SERVER_NAME} rank={payload.get('rank')} "
              f"offset={clock_offset_ms}ms")
    except Exception as exc:
        print(f"[HEARTBEAT] falha ao sincronizar com referência: {exc}")
        schedule_registration_retry(exc)


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


def validate_required_string(payload: dict, field: str):
    """Contrato de campo string obrigatorio (ver PROTOCOL.md, PUBLISH_REQUEST).

    Retorna None se valido, ou a mensagem de erro. SEM conversao implicita:
      - ausente, null ou string vazia -> "campo 'X' ausente ou vazio"
      - qualquer outro tipo (numero, booleano, lista, objeto) ->
        "campo 'X' deve ser uma string"
    O teste de tipo vem ANTES do teste de vazio de proposito: valores
    "falsy" como 0, false, [] e {} sao do tipo errado, nao "vazios".
    """
    value = payload.get(field)
    if value is None:
        return f"campo '{field}' ausente ou vazio"
    if not isinstance(value, str):
        return f"campo '{field}' deve ser uma string"
    if value == "":
        return f"campo '{field}' ausente ou vazio"
    return None


def handle_publish_request(envelope: dict) -> dict:
    """Trata PUBLISH_REQUEST: valida canal, persiste e so entao publica
    no broker (ver ordem persistencia->publicacao documentada em
    protocol/PROTOCOL.md). Devolve PUBLISH_RESPONSE via REQ/REP.

    Nenhuma requisicao invalida chega a persistencia nem ao broker: toda
    validacao acontece antes do primeiro append_message."""
    payload = envelope.get("payload") or {}

    for field in ("channel_name", "message"):
        error = validate_required_string(payload, field)
        if error:
            return build_envelope(
                "PUBLISH_RESPONSE", {"status": "ERROR", "error_msg": error}
            )

    channel_name = payload["channel_name"]
    message = payload["message"]

    channel_exists = any(c.get("channel_name") == channel_name for c in known_channels)
    if not channel_exists:
        return build_envelope(
            "PUBLISH_RESPONSE",
            {"status": "ERROR", "error_msg": f"canal '{channel_name}' nao existe"},
        )

    record = {
        "channel_name": channel_name,
        "sender_id": envelope.get("sender_id"),
        "sender_lang": envelope.get("sender_lang"),
        "message": message,
        "timestamp": envelope.get("timestamp"),
    }
    # 1) Persiste primeiro (durabilidade antes de visibilidade).
    persistence.append_message(record, MESSAGES_PATH)

    # 2) So entao publica no broker, para subscribers ao vivo.
    publish_envelope = build_envelope(
        "CHANNEL_MESSAGE", {"channel_name": channel_name, "message": message}
    )
    topic = channel_name.encode("utf-8")
    body = msgpack.packb(publish_envelope, use_bin_type=True)
    if pub_socket is not None:
        try:
            pub_socket.send_multipart([topic, body])
        except Exception as e:
            # A mensagem JA esta persistida (passo 1), entao nao perdemos
            # dado -- so o subscriber ao vivo que nao recebe. Devolver erro
            # ao client aqui faria um retry gerar mensagem duplicada em
            # disco. Mesmo comportamento do Server.java.
            print(f"[ERROR] Falha ao publicar no broker: {e}")

    print(f"[PUBLISH] channel='{channel_name}' message='{message}'")

    return build_envelope("PUBLISH_RESPONSE", {"status": "OK"})


def dispatch(envelope: dict) -> dict:
    """Roteia o envelope recebido para o handler correto pelo campo 'type'."""
    msg_type = envelope.get("type")

    if msg_type == "LOGIN_REQUEST":
        return handle_login_request(envelope)

    if msg_type == "CHANNEL_CREATE_REQUEST":
        return handle_channel_create_request(envelope)

    if msg_type == "CHANNEL_LIST_REQUEST":
        return handle_channel_list_request(envelope)

    if msg_type == "PUBLISH_REQUEST":
        return handle_publish_request(envelope)

    # Tipo desconhecido nao deve derrubar o servidor (REP exige sempre 1
    # send() por recv(), senao o socket trava em estado inconsistente).
    return build_envelope(
        "ERROR_RESPONSE",
        {"status": "ERROR", "error_msg": f"tipo de mensagem desconhecido: {msg_type}"},
    )


def main():
    global known_logins, known_channels, pub_socket, reference_socket
    global client_messages_received, zmq_context
    known_logins = persistence.load_logins(LOGINS_PATH)
    known_channels = persistence.load_channels(CHANNELS_PATH)
    known_messages = persistence.load_messages(MESSAGES_PATH)
    print(
        f"[SERVER] Historico carregado: {len(known_logins)} login(s), "
        f"{len(known_channels)} canal(is) e {len(known_messages)} "
        f"mensagem(ns) previamente persistido(s)"
    )

    context = zmq.Context()
    zmq_context = context
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.RCVTIMEO, IDLE_TICK_MS)
    socket.bind(BIND_ADDRESS)
    print(f"[SERVER] Python Server (REP) ouvindo em {BIND_ADDRESS}")

    # PUB conectado ao broker (Parte 2). "connect", nao "bind": quem faz
    # bind e o broker (XSUB), o servidor so se conecta nele.
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(BROKER_XSUB_ADDRESS)
    print(f"[SERVER] PUB conectado ao broker em {BROKER_XSUB_ADDRESS}")

    reference_socket = new_reference_socket()
    # O registro inicial NAO e mais fatal: se o Reference estiver fora do ar, o
    # servidor sobe mesmo assim e tenta de novo (backoff) a partir do loop.
    if not try_register():
        print("[SERVER] iniciado SEM registro no Reference Service; "
              "novas tentativas automaticas em andamento")

    try:
        while True:
            try:
                raw = socket.recv()
            except zmq.Again:
                maybe_register()  # ocioso: aproveita para tentar o registro
                continue
            client_messages_received += 1
            if client_messages_received % HEARTBEAT_EVERY == 0:
                send_heartbeat()

            # Cada mensagem e tratada isoladamente: qualquer falha ao
            # decodificar o MessagePack, ou ao processar o envelope
            # (campos ausentes, tipos inesperados, envelope que nao e um
            # dict, etc.), e capturada aqui e vira um ERROR_RESPONSE, em
            # vez de propagar e derrubar o servidor inteiro. O socket REP
            # exige exatamente 1 send() por recv(), entao SEMPRE
            # respondemos algo, mesmo em erro.
            try:
                envelope = msgpack.unpackb(raw, raw=False)
                observe_message(envelope)
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
            maybe_register()  # depois de responder: a tentativa nao atrasa o client
    except KeyboardInterrupt:
        print("\n[SERVER] Encerrando...")
    finally:
        socket.close()
        pub_socket.close()
        reference_socket.close()
        context.term()


if __name__ == "__main__":
    main()
