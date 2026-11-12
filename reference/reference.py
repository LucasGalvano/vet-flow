"""Reference Service (Partes 3 e 4). Atende somente servidores via REQ/REP.

Parte 4: a resposta NAO traz mais a hora (`reference_time_ms`); a sincronizacao
de relogio agora e feita com o coordenador eleito entre os servidores. O servico
continua informando rank, lista de servidores disponiveis e heartbeat."""

import os
import time

import msgpack
import zmq

from registry import ServerRegistry

BIND_ADDRESS = os.environ.get("REFERENCE_BIND_ADDRESS", "tcp://*:5559")
TIMEOUT_SECONDS = float(os.environ.get("REFERENCE_TIMEOUT_SECONDS", "30"))
# Maior contador remoto aceito: Long.MAX_VALUE - 1 (folga para o +1 do envio).
MAX_LOGICAL_CLOCK = 2**63 - 2


def main():
    registry = ServerRegistry(TIMEOUT_SECONDS)
    logical_clock = 0
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(BIND_ADDRESS)
    print(f"[REFERENCE] ouvindo em {BIND_ADDRESS}; timeout={TIMEOUT_SECONDS}s")

    try:
        while True:
            raw = socket.recv()
            try:
                request = msgpack.unpackb(raw, raw=False)
                if not isinstance(request, dict):
                    raise ValueError("envelope deve ser um mapa")
                received_clock = request.get("logical_clock", 0)
                # Ignora contador invalido/fora de faixa (ver MAX_LOGICAL_CLOCK):
                # 2**64-1 estourava o uint64 do MessagePack e derrubava o servico.
                if (isinstance(received_clock, int) and not isinstance(received_clock, bool)
                        and 0 <= received_clock <= MAX_LOGICAL_CLOCK):
                    logical_clock = max(logical_clock, received_clock)

                payload = request.get("payload") or {}
                request_type = request.get("type")
                server_name = payload.get("server_name")
                response_payload = {"status": "OK"}

                if request_type == "REFERENCE_REGISTER_REQUEST":
                    if not isinstance(server_name, str) or not server_name:
                        raise ValueError("server_name ausente ou inválido")
                    response_payload["rank"] = registry.register(server_name)
                elif request_type == "REFERENCE_LIST_REQUEST":
                    response_payload["servers"] = registry.available()
                elif request_type == "REFERENCE_HEARTBEAT_REQUEST":
                    if not isinstance(server_name, str) or not server_name:
                        raise ValueError("server_name ausente ou inválido")
                    response_payload["rank"] = registry.heartbeat(server_name)
                else:
                    response_payload = {
                        "status": "ERROR",
                        "error_msg": f"tipo desconhecido: {request_type}",
                    }
                logical_clock += 1
                response = {
                    "type": "REFERENCE_RESPONSE",
                    "sender_id": "reference-service",
                    "sender_lang": "python",
                    "timestamp": int(time.time() * 1000),
                    "logical_clock": logical_clock,
                    "payload": response_payload,
                }
                out = msgpack.packb(response, use_bin_type=True)
                print(f"[RECV] {request}\n[SEND] {response}")
            except Exception as exc:
                logical_clock += 1
                response = {
                    "type": "REFERENCE_RESPONSE",
                    "sender_id": "reference-service",
                    "sender_lang": "python",
                    "timestamp": int(time.time() * 1000),
                    "logical_clock": logical_clock,
                    "payload": {"status": "ERROR", "error_msg": str(exc)},
                }
                out = msgpack.packb(response, use_bin_type=True)
            socket.send(out)
    except KeyboardInterrupt:
        print("\n[REFERENCE] encerrando")
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()
