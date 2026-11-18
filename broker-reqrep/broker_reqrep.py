"""
Broker REQ/REP - round-robin entre os servidores.

Bots (REQ) conectam no ROUTER (frontend). Servidores (REP) conectam no DEALER
(backend). O DEALER distribui as requisicoes entre os servidores conectados em
round-robin e devolve cada resposta ao bot que a pediu. Sem logica de negocio.

Fluxo: Bots --REQ--> [ROUTER broker DEALER] --REQ--> Servidores (REP)
"""

import os

import zmq

FRONTEND_BIND = os.environ.get("REQREP_FRONTEND_BIND", "tcp://*:5555")
BACKEND_BIND = os.environ.get("REQREP_BACKEND_BIND", "tcp://*:5556")


def main():
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(FRONTEND_BIND)
    backend = context.socket(zmq.DEALER)
    backend.bind(BACKEND_BIND)
    print(f"[BROKER-REQREP] bots (ROUTER) em {FRONTEND_BIND}; servidores (DEALER) em {BACKEND_BIND}")
    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        print("\n[BROKER-REQREP] Encerrando...")
    finally:
        frontend.close()
        backend.close()
        context.term()


if __name__ == "__main__":
    main()
