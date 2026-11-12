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
# Backend do broker REQ/REP (ROUTER/DEALER). Opcional: se definido, o REP tambem
# se CONECTA ao broker (alem do bind), e passa a receber requisicoes em round-robin.
SERVER_BROKER_ADDRESS = os.environ.get("SERVER_BROKER_ADDRESS", "")
SERVER_NAME = os.environ.get("SERVER_NAME", "python-server")
HEARTBEAT_EVERY = int(os.environ.get("HEARTBEAT_EVERY", "10"))

# --- Parte 4: eleicao (bully) e sincronizacao de relogio com o coordenador ---
# Mensagens de clientes entre duas sincronizacoes com o coordenador.
SYNC_EVERY = int(os.environ.get("SYNC_EVERY", "15"))
# XPUB do broker Pub/Sub: o servidor assina o topico `servers` para saber quem e o coordenador.
BROKER_XPUB_ADDRESS = os.environ.get("BROKER_XPUB_ADDRESS", "tcp://localhost:5558")
# Porta em que os servidores atendem uns aos outros (a mesma do REP: 5555). O endereco de
# um par e tcp://<nome do servidor>:PEER_PORT (o nome registrado no Reference e o hostname).
PEER_PORT = int(os.environ.get("PEER_PORT", "5555"))
PEER_TIMEOUT_MS = max(1, int(os.environ.get("PEER_TIMEOUT_MS", "1000")))
# Quanto esperar o anuncio do coordenador depois que um servidor de rank maior respondeu OK.
ELECTION_WAIT_SECONDS = float(os.environ.get("ELECTION_WAIT_SECONDS", "5"))
# O coordenador republica o anuncio periodicamente: o Pub/Sub nao garante entrega a quem
# acabou de assinar (slow joiner) e isso tambem corrige coordenadores divergentes.
ANNOUNCE_EVERY_SECONDS = float(os.environ.get("COORDINATOR_ANNOUNCE_SECONDS", "5"))
# Desvio artificial do relogio local (para demonstrar a sincronizacao: no Docker todos os
# containers compartilham o mesmo relogio fisico).
CLOCK_SKEW_MS = int(os.environ.get("CLOCK_SKEW_MS", "0"))
SERVERS_TOPIC = "servers"
PEER_TYPES = ("ELECTION_REQUEST", "CLOCK_REQUEST")
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
clock_adjust_ms = 0  # ajuste acumulado pela sincronizacao com o coordenador
# Parte 4
coordinator = None  # nome do servidor eleito coordenador (None = desconhecido)
election_due = False
election_not_before = 0.0
awaiting_announcement_until = None  # monotonic; None = nao esta esperando anuncio
announce_due = False
next_announcement_at = 0.0
sync_due = False
sub_socket = None
client_messages_received = 0
server_rank = None
zmq_context = None
reference_registered = False
registration_retry_delay = REFERENCE_RETRY_INITIAL_SECONDS
next_registration_attempt = 0.0


def local_clock_ms() -> int:
    """Relogio local deste servidor (com o desvio simulado CLOCK_SKEW_MS)."""
    return int(time.time() * 1000) + CLOCK_SKEW_MS


def physical_timestamp_ms() -> int:
    """Hora fisica usada nos timestamps: relogio local + ajuste do coordenador."""
    return local_clock_ms() + clock_adjust_ms


# Maior contador remoto aceito: Long.MAX_VALUE - 1, deixando folga para o +1 do
# proximo envio. Valores fora de [0, MAX_LOGICAL_CLOCK] (negativos, booleanos,
# ou >= 2**63) sao IGNORADOS em vez de adotados: um contador de 2**64-1 fazia o
# proximo `+= 1` estourar o uint64 do MessagePack e derrubar o processo.
MAX_LOGICAL_CLOCK = 2**63 - 2


def observe_message(envelope: dict) -> None:
    global logical_clock
    value = envelope.get("logical_clock", 0)
    if (isinstance(value, int) and not isinstance(value, bool)
            and 0 <= value <= MAX_LOGICAL_CLOCK):
        logical_clock = max(logical_clock, value)


def build_envelope(msg_type: str, payload: dict) -> dict:
    """Monta um envelope no formato do protocolo comum (ver PROTOCOL.md)."""
    global logical_clock
    logical_clock += 1
    return {
        "type": msg_type,
        "sender_id": SERVER_NAME,
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
    print(f"[REF-SEND] {request}")
    try:
        reference_socket.send(msgpack.packb(request, use_bin_type=True))
        raw_response = reference_socket.recv()
    except zmq.ZMQError:
        reset_reference_socket()
        raise
    response = msgpack.unpackb(raw_response, raw=False)
    observe_message(response)
    print(f"[REF-RECV] {response}")
    response_payload = response.get("payload") or {}
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
    print(f"[SERVER] registrado como {SERVER_NAME}, rank={server_rank}")
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
        print(f"[HEARTBEAT] servidor={SERVER_NAME} rank={payload.get('rank')}")
    except Exception as exc:
        print(f"[HEARTBEAT] falha ao sincronizar com referência: {exc}")
        schedule_registration_retry(exc)


# ---------------------------------------------------------------------------
# Parte 4: eleicao (bully) e sincronizacao de relogio com o coordenador
#
# Tudo roda no loop unico do REP (sem threads), como o resto do servidor:
#  - ELECTION_REQUEST / CLOCK_REQUEST chegam no mesmo REP (porta 5555) e sao tratados
#    por dispatch(); o servidor so inicia trocas com servidores de rank MAIOR (eleicao)
#    ou com o coordenador (relogio), entao as esperas formam uma cadeia, nao um ciclo.
#  - O anuncio do coordenador chega pelo topico `servers` (socket SUB no broker Pub/Sub).
# ---------------------------------------------------------------------------

def peer_request(name: str, msg_type: str, payload: dict) -> dict:
    """REQ/REP direto com outro servidor. Socket descartavel por troca: um REQ que
    estoura o timeout fica inutilizavel (mesma razao do Reference)."""
    sock = zmq_context.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, PEER_TIMEOUT_MS)
    sock.setsockopt(zmq.SNDTIMEO, PEER_TIMEOUT_MS)
    sock.setsockopt(zmq.LINGER, 0)
    try:
        sock.connect(f"tcp://{name}:{PEER_PORT}")
        request = build_envelope(msg_type, payload)
        print(f"[PEER-SEND] para={name} {request}")
        sock.send(msgpack.packb(request, use_bin_type=True))
        response = msgpack.unpackb(sock.recv(), raw=False)
    finally:
        sock.close()
    observe_message(response)
    print(f"[PEER-RECV] de={name} {response}")
    return response


def other_servers() -> list:
    """Servidores disponiveis segundo o Reference, exceto este."""
    payload = reference_request("REFERENCE_LIST_REQUEST", {})
    return [s for s in payload.get("servers", []) if s.get("name") != SERVER_NAME]


def announce_coordinator() -> None:
    """Publica no topico `servers` o nome do coordenador."""
    global announce_due, next_announcement_at
    announce_due = False
    next_announcement_at = time.monotonic() + ANNOUNCE_EVERY_SECONDS
    envelope = build_envelope(
        "COORDINATOR_ANNOUNCEMENT", {"coordinator": SERVER_NAME, "rank": server_rank}
    )
    print(f"[PUB-SEND] topic={SERVERS_TOPIC} {envelope}")
    try:
        pub_socket.send_multipart([
            SERVERS_TOPIC.encode("utf-8"),
            msgpack.packb(envelope, use_bin_type=True),
        ])
    except Exception as exc:
        print(f"[ERROR] Falha ao anunciar o coordenador: {exc}")


def become_coordinator() -> None:
    global coordinator, awaiting_announcement_until
    coordinator = SERVER_NAME
    awaiting_announcement_until = None
    print(f"[COORDENADOR] {SERVER_NAME} (rank {server_rank}) eleito coordenador")
    announce_coordinator()


def run_election() -> None:
    """Eleicao pelo algoritmo do valentao (bully): pede eleicao aos servidores de rank
    maior. Se algum responde OK, espera o anuncio dele; se nenhum responde, este servidor
    e o novo coordenador."""
    global election_due, election_not_before, coordinator, awaiting_announcement_until
    election_due = False
    if not isinstance(server_rank, int):
        election_due = True
        election_not_before = time.monotonic() + 1.0
        return
    try:
        servers = other_servers()
    except Exception as exc:
        print(f"[ELEICAO] nao foi possivel listar os servidores no Reference: {exc}")
        election_due = True
        election_not_before = time.monotonic() + 1.0
        return

    higher = sorted(
        (s for s in servers if isinstance(s.get("rank"), int) and s["rank"] > server_rank),
        key=lambda s: s["rank"],
    )
    print(f"[ELEICAO] {SERVER_NAME} (rank {server_rank}) inicia eleicao; "
          f"servidores de rank maior: {[s['name'] for s in higher]}")
    for server in higher:
        try:
            response = peer_request(server["name"], "ELECTION_REQUEST", {"rank": server_rank})
        except Exception as exc:
            print(f"[ELEICAO] {server['name']} nao respondeu ({exc})")
            continue
        if (response.get("payload") or {}).get("status") == "OK":
            coordinator = None
            awaiting_announcement_until = time.monotonic() + ELECTION_WAIT_SECONDS
            print(f"[ELEICAO] {server['name']} respondeu OK; aguardando o anuncio do coordenador")
            return
    become_coordinator()


def handle_coordinator_announcement(envelope: dict) -> None:
    global coordinator, awaiting_announcement_until, election_due, announce_due, sync_due
    payload = envelope.get("payload") or {}
    name = payload.get("coordinator")
    rank = payload.get("rank")
    if not isinstance(name, str) or name == SERVER_NAME:
        return
    if isinstance(rank, int) and isinstance(server_rank, int) and rank < server_rank:
        # Anuncio de um servidor de rank menor (ex.: antigo coordenador que ainda nao sabe
        # que um servidor maior voltou): este servidor deve assumir.
        if coordinator == SERVER_NAME:
            announce_due = True
        else:
            election_due = True
        return
    if coordinator != name:
        print(f"[COORDENADOR] novo coordenador: {name} (rank {rank})")
        sync_due = True  # sincroniza logo, sem esperar as proximas mensagens
    coordinator = name
    awaiting_announcement_until = None
    election_due = False


def drain_announcements() -> None:
    """Le (sem bloquear) o que chegou no topico `servers`."""
    while True:
        try:
            topic, body = sub_socket.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.Again:
            return
        try:
            envelope = msgpack.unpackb(body, raw=False)
            observe_message(envelope)
            topic_name = topic.decode("utf-8")
            print(f"[PUBSUB-RECV] topic={topic_name} {envelope}")
            # O ZeroMQ casa topicos por prefixo: confere o nome inteiro.
            if topic_name == SERVERS_TOPIC and envelope.get("type") == "COORDINATOR_ANNOUNCEMENT":
                handle_coordinator_announcement(envelope)
        except Exception as exc:
            print(f"[ERROR] Falha ao processar mensagem do topico servers: {exc}")


def sync_clock() -> None:
    """Pede a hora ao coordenador e ajusta o relogio (compensando meia latencia). Se o
    coordenador nao responde, ele e considerado indisponivel e uma eleicao e iniciada."""
    global sync_due, clock_adjust_ms, coordinator, election_due
    sync_due = False
    if coordinator is None:
        return  # sem coordenador conhecido; housekeeping() cuida da eleicao
    if coordinator == SERVER_NAME:
        print(f"[CLOCK-SYNC] {SERVER_NAME} e o coordenador: relogio de referencia "
              f"(ajuste={clock_adjust_ms}ms)")
        return
    t0 = local_clock_ms()
    try:
        response = peer_request(coordinator, "CLOCK_REQUEST", {})
    except Exception as exc:
        print(f"[CLOCK-SYNC] coordenador {coordinator} indisponivel ({exc}); iniciando eleicao")
        coordinator = None
        election_due = True
        return
    t1 = local_clock_ms()
    payload = response.get("payload") or {}
    coordinator_time = payload.get("time_ms")
    if payload.get("status") != "OK" or not isinstance(coordinator_time, int):
        print(f"[CLOCK-SYNC] {coordinator} nao e mais o coordenador "
              f"({payload.get('error_msg')}); iniciando eleicao")
        coordinator = None
        election_due = True
        return
    # A hora do coordenador foi lida, em media, no meio da troca (estimativa de Cristian).
    new_adjust = coordinator_time + (t1 - t0) // 2 - t1
    delta = new_adjust - clock_adjust_ms
    clock_adjust_ms = new_adjust
    print(f"[CLOCK-SYNC] coordenador={coordinator} rtt={t1 - t0}ms "
          f"correcao={delta:+d}ms ajuste_total={clock_adjust_ms}ms")


def handle_election_request(envelope: dict) -> dict:
    """ELECTION_REQUEST de um servidor de rank menor: responde OK e assume a eleicao."""
    global election_due, announce_due
    sender_rank = (envelope.get("payload") or {}).get("rank")
    if not (isinstance(sender_rank, int) and isinstance(server_rank, int)
            and sender_rank > server_rank):
        if coordinator == SERVER_NAME:
            announce_due = True   # ja sou o coordenador: so reafirma
        else:
            election_due = True   # depois de responder, disputa a coordenacao
    return build_envelope("ELECTION_RESPONSE", {"status": "OK", "rank": server_rank})


def handle_clock_request(envelope: dict) -> dict:
    """CLOCK_REQUEST: so o coordenador responde com a hora correta."""
    if coordinator != SERVER_NAME:
        return build_envelope("CLOCK_RESPONSE", {
            "status": "ERROR",
            "error_msg": "este servidor nao e o coordenador",
            "coordinator": coordinator,
        })
    return build_envelope("CLOCK_RESPONSE", {"status": "OK", "time_ms": physical_timestamp_ms()})


def housekeeping() -> None:
    """Tarefas periodicas do loop principal: registro no Reference, eleicao, reanuncio do
    coordenador e sincronizacao de relogio."""
    global election_due, awaiting_announcement_until
    maybe_register()
    if not reference_registered:
        return
    now = time.monotonic()
    if coordinator is None:
        if awaiting_announcement_until is None:
            election_due = True  # ninguem conhecido e nenhuma eleicao em andamento
        elif now >= awaiting_announcement_until:
            print("[ELEICAO] o anuncio do coordenador nao chegou a tempo; refazendo a eleicao")
            awaiting_announcement_until = None
            election_due = True
    if election_due and now >= election_not_before:
        run_election()
    if coordinator == SERVER_NAME and (announce_due or time.monotonic() >= next_announcement_at):
        announce_coordinator()
    if sync_due:
        sync_clock()
        if election_due and time.monotonic() >= election_not_before:
            run_election()  # o coordenador caiu durante a sincronizacao


def after_client_message() -> None:
    """Chamado depois de responder uma mensagem de cliente (nao atrasa a resposta)."""
    global sync_due
    if client_messages_received % HEARTBEAT_EVERY == 0:
        send_heartbeat()
    if client_messages_received % SYNC_EVERY == 0:
        sync_due = True


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

    # `servers` e o topico do anuncio do coordenador (Parte 4): nao pode virar canal.
    if channel_name == SERVERS_TOPIC:
        return build_envelope(
            "CHANNEL_CREATE_RESPONSE",
            {"status": "ERROR", "error_msg": f"nome de canal reservado: '{SERVERS_TOPIC}'"},
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
    print(f"[PUB-SEND] topic={channel_name} {publish_envelope}")
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

    # Parte 4: trocas entre servidores.
    if msg_type == "ELECTION_REQUEST":
        return handle_election_request(envelope)

    if msg_type == "CLOCK_REQUEST":
        return handle_clock_request(envelope)

    # Tipo desconhecido nao deve derrubar o servidor (REP exige sempre 1
    # send() por recv(), senao o socket trava em estado inconsistente).
    return build_envelope(
        "ERROR_RESPONSE",
        {"status": "ERROR", "error_msg": f"tipo de mensagem desconhecido: {msg_type}"},
    )


def main():
    global known_logins, known_channels, pub_socket, reference_socket, sub_socket
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
    if SERVER_BROKER_ADDRESS:
        socket.connect(SERVER_BROKER_ADDRESS)
        print(f"[SERVER] REP tambem conectado ao broker REQ/REP em {SERVER_BROKER_ADDRESS}")

    # PUB conectado ao broker (Parte 2). "connect", nao "bind": quem faz
    # bind e o broker (XSUB), o servidor so se conecta nele.
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(BROKER_XSUB_ADDRESS)
    print(f"[SERVER] PUB conectado ao broker em {BROKER_XSUB_ADDRESS}")

    # SUB no XPUB do broker (Parte 4): recebe o anuncio do coordenador no topico `servers`.
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(BROKER_XPUB_ADDRESS)
    sub_socket.setsockopt(zmq.SUBSCRIBE, SERVERS_TOPIC.encode("utf-8"))
    print(f"[SERVER] SUB conectado ao broker em {BROKER_XPUB_ADDRESS}, "
          f"assinando o topico '{SERVERS_TOPIC}'")

    reference_socket = new_reference_socket()
    # O registro inicial NAO e fatal: se o Reference estiver fora do ar, o
    # servidor sobe mesmo assim e tenta de novo (backoff) a partir do loop.
    if not try_register():
        print("[SERVER] iniciado SEM registro no Reference Service; "
              "novas tentativas automaticas em andamento")

    try:
        while True:
            drain_announcements()
            try:
                raw = socket.recv()
            except zmq.Again:
                housekeeping()  # ocioso: registro, eleicao, reanuncio e sincronizacao
                continue

            # Cada mensagem e tratada isoladamente: qualquer falha ao
            # decodificar o MessagePack, ou ao processar o envelope
            # (campos ausentes, tipos inesperados, envelope que nao e um
            # dict, etc.), e capturada aqui e vira um ERROR_RESPONSE, em
            # vez de propagar e derrubar o servidor inteiro. O socket REP
            # exige exatamente 1 send() por recv(), entao SEMPRE
            # respondemos algo, mesmo em erro.
            is_peer_message = False
            try:
                envelope = msgpack.unpackb(raw, raw=False)
                observe_message(envelope)
                print(f"[RECV] {envelope}")
                is_peer_message = (isinstance(envelope, dict)
                                   and envelope.get("type") in PEER_TYPES)
                response = dispatch(envelope)
                out = msgpack.packb(response, use_bin_type=True)
            except Exception as e:
                print(f"[ERROR] Falha ao processar mensagem: {e}")
                response = build_envelope(
                    "ERROR_RESPONSE",
                    {
                        "status": "ERROR",
                        "error_msg": f"mensagem invalida ou mal-formada: {e}",
                    },
                )
                out = msgpack.packb(response, use_bin_type=True)

            socket.send(out)
            print(f"[SEND] {response}")

            # Depois de responder: nada abaixo atrasa o cliente que acabou de ser atendido.
            if not is_peer_message:
                client_messages_received += 1
                after_client_message()
            housekeeping()
    except KeyboardInterrupt:
        print("\n[SERVER] Encerrando...")
    finally:
        socket.close()
        pub_socket.close()
        sub_socket.close()
        reference_socket.close()
        context.term()


if __name__ == "__main__":
    main()
