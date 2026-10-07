"""
Persistencia simples para o Python Server - Parte 1 (passo 3)

Persiste dois tipos de dado, cada um em seu proprio arquivo MessagePack:
    - logins:   bot_name + sender_lang + timestamp
    - channels: channel_name + created_by + timestamp

Formato escolhido (ver decisao registrada no historico do projeto):
    Cada arquivo contem uma LISTA de registros. A cada escrita: le a
    lista inteira, adiciona o registro, regrava o arquivo inteiro.

    Isso e intencionalmente simples. Nao ha banco de dados, nao ha
    escrita incremental/append-only. Para o volume de um projeto
    academico isso e adequado; nao e otimizado para grande volume de
    escritas (cada escrita reescreve o arquivo inteiro).

Este modulo e "burro" de proposito: ele so sabe carregar/salvar listas
em disco. Regras de negocio (ex.: impedir nome de canal duplicado) NAO
ficam aqui -- ficam no server.py, que decide o que fazer antes de
chamar append_channel/append_login.

Concorrencia: este modulo assume um UNICO processo, single-threaded,
processando uma requisicao REP por vez (e assim que o server.py atual
funciona). Se o server for alterado para processar requisicoes em
paralelo (ex.: multithreading), este modulo precisara de um lock de
arquivo — isso NAO esta implementado aqui porque nao e o caso atual.
"""

import os

import msgpack

DEFAULT_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DEFAULT_LOGINS_PATH = os.path.join(DEFAULT_DATA_DIR, "logins.msgpack")
DEFAULT_CHANNELS_PATH = os.path.join(DEFAULT_DATA_DIR, "channels.msgpack")
DEFAULT_MESSAGES_PATH = os.path.join(DEFAULT_DATA_DIR, "messages.msgpack")


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _load_list(path: str) -> list:
    """Carrega uma lista persistida de um arquivo MessagePack.

    Retorna [] se o arquivo ainda nao existe (primeira execucao),
    estiver vazio, ou corrompido/truncado -- nesses casos o servidor
    deve poder subir normalmente, apenas com historico vazio, em vez
    de falhar ao iniciar.
    """
    if not os.path.exists(path):
        return []

    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError:
        return []

    if not raw:
        return []

    try:
        data = msgpack.unpackb(raw, raw=False)
    except (msgpack.exceptions.UnpackException, ValueError):
        # Arquivo corrompido/truncado: nao derruba o servidor, apenas
        # comeca com lista vazia. Decisao conservadora para nao
        # bloquear a demonstracao por causa de um arquivo ruim.
        return []

    return data if isinstance(data, list) else []


def _save_list(items: list, path: str) -> None:
    """Regrava o arquivo inteiro com a lista atual de itens."""
    _ensure_parent_dir(path)
    packed = msgpack.packb(items, use_bin_type=True)
    with open(path, "wb") as f:
        f.write(packed)


def _append_item(record: dict, path: str) -> list:
    """Adiciona um registro a lista persistida em `path` e a resalva.

    Retorna a lista completa e atualizada, para o chamador manter em
    memoria sem precisar reler o arquivo em seguida.
    """
    items = _load_list(path)
    items.append(record)
    _save_list(items, path)
    return items


# ---------------------------------------------------------------------
# Logins
# ---------------------------------------------------------------------

def load_logins(path: str = DEFAULT_LOGINS_PATH) -> list:
    return _load_list(path)


def save_logins(logins: list, path: str = DEFAULT_LOGINS_PATH) -> None:
    _save_list(logins, path)


def append_login(record: dict, path: str = DEFAULT_LOGINS_PATH) -> list:
    return _append_item(record, path)


# ---------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------

def load_channels(path: str = DEFAULT_CHANNELS_PATH) -> list:
    return _load_list(path)


def save_channels(channels: list, path: str = DEFAULT_CHANNELS_PATH) -> None:
    _save_list(channels, path)


def append_channel(record: dict, path: str = DEFAULT_CHANNELS_PATH) -> list:
    return _append_item(record, path)


# ---------------------------------------------------------------------
# Messages (Parte 2 -- mensagens publicadas em canais)
# ---------------------------------------------------------------------

def load_messages(path: str = DEFAULT_MESSAGES_PATH) -> list:
    return _load_list(path)


def save_messages(messages: list, path: str = DEFAULT_MESSAGES_PATH) -> None:
    _save_list(messages, path)


def append_message(record: dict, path: str = DEFAULT_MESSAGES_PATH) -> list:
    return _append_item(record, path)