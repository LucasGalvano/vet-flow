"""Consulta o Reference Service e imprime os servidores disponiveis.

Uso: python tests/list_servers.py [tcp://localhost:5559]
Requer: pip install pyzmq msgpack
"""

import sys
import time

import msgpack
import zmq

addr = sys.argv[1] if len(sys.argv) > 1 else "tcp://localhost:5559"
sock = zmq.Context().socket(zmq.REQ)
sock.setsockopt(zmq.RCVTIMEO, 3000)
sock.setsockopt(zmq.LINGER, 0)
sock.connect(addr)

request = {
    "type": "REFERENCE_LIST_REQUEST",
    "sender_id": "tester",
    "sender_lang": "python",
    "timestamp": int(time.time() * 1000),
    "logical_clock": 1,
    "payload": {},
}
sock.send(msgpack.packb(request, use_bin_type=True))
response = msgpack.unpackb(sock.recv(), raw=False)
for server in response["payload"].get("servers", []):
    print(f"rank={server['rank']} nome={server['name']}")
if not response["payload"].get("servers"):
    print("(nenhum servidor disponivel)")
