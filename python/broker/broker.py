"""
Broker Pub/Sub - Parte 2
Sistema distribuido de mensagens - dominio: clinica veterinaria

Processo SEPARADO dos servidores, sem nenhuma logica de negocio: apenas
encaminha mensagens entre os PUB dos servidores e os SUB dos clients,
usando o proxy pronto do ZeroMQ (zmq.proxy).

Conforme especificado:
    - XSUB na porta 5557 (lado dos publishers -- servidores conectam aqui)
    - XPUB na porta 5558 (lado dos subscribers -- clients conectam aqui)

Fluxo: Servers --PUB--> [XSUB broker XPUB] --SUB--> Clients
"""

import os

import zmq

XSUB_BIND = os.environ.get("BROKER_XSUB_BIND", "tcp://*:5557")
XPUB_BIND = os.environ.get("BROKER_XPUB_BIND", "tcp://*:5558")


def main():
    context = zmq.Context()

    xsub = context.socket(zmq.XSUB)
    xsub.bind(XSUB_BIND)

    xpub = context.socket(zmq.XPUB)
    xpub.bind(XPUB_BIND)

    print(f"[BROKER] XSUB (publishers/servidores) ouvindo em {XSUB_BIND}")
    print(f"[BROKER] XPUB (subscribers/clients) ouvindo em {XPUB_BIND}")

    try:
        # zmq.proxy bloqueia aqui, encaminhando frames em ambas as
        # direcoes (incluindo as subscricoes dos clients, que o XPUB
        # propaga de volta para o XSUB -- e assim que o proxy sabe quais
        # topicos existem interesse, mecanismo interno do ZeroMQ).
        zmq.proxy(xsub, xpub)
    except KeyboardInterrupt:
        print("\n[BROKER] Encerrando...")
    finally:
        xsub.close()
        xpub.close()
        context.term()


if __name__ == "__main__":
    main()