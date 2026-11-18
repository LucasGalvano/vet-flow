"""
Regressao: contador logico FORA DE FAIXA recebido de um client/servidor nao pode
derrubar o processo nem fazer o relogio andar para tras.

Antes da correcao, uma unica mensagem com logical_clock = 2**64-1 fazia o
proximo `+= 1` estourar o uint64 do MessagePack: o servidor Python (e o
Reference) morriam com OverflowError e nunca mais respondiam.

  python tests/test_part3_clock_bounds.py                          # servidor Python + Reference
  python tests/test_part3_clock_bounds.py --java-jar <vetflow-server.jar>

Checks por alvo (servidor e Reference):
  1. responde a cada valor extremo (nao cai, nao trava);
  2. o logical_clock da resposta e inteiro >= 1;
  3. a sequencia de respostas nunca diminui (monotonico);
  4. continua respondendo normalmente depois.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_part3_battery as b  # noqa: E402

EXTREMES = [2**63 - 1, 2**64 - 1, -5, True, 0]


def probe(label, addr, msg_type, payload):
    ok_all = True
    last = 0
    for v in EXTREMES:
        try:
            r = b.request(addr, msg_type, payload, timeout_ms=3000, clock=v)
            c = r.get("logical_clock")
        except Exception as ex:  # timeout = processo morto/travado
            b.record(label, f"responde a logical_clock={v!r}", False, f"sem resposta ({type(ex).__name__})")
            return False
        good = isinstance(c, int) and not isinstance(c, bool) and c >= 1
        mono = good and c >= last
        b.record(label, f"logical_clock={v!r}: resposta com contador >= 1 e sem regredir (anterior={last})",
                 good and mono, f"contador da resposta={c}")
        ok_all &= good and mono
        if good:
            last = c
    try:
        r = b.request(addr, msg_type, payload, timeout_ms=3000, clock=1)
        b.record(label, "continua respondendo normalmente depois", isinstance(r, dict))
    except Exception as ex:
        b.record(label, "continua respondendo normalmente depois", False, f"sem resposta ({type(ex).__name__})")
        ok_all = False
    return ok_all


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--java-jar")
    args = ap.parse_args()
    args.server_cmd = None
    if args.java_jar:
        args.java_jar = os.path.abspath(args.java_jar)
    kind = "JAVA" if args.java_jar else "PYTHON"
    print(f"=== Contador logico fora de faixa | servidor: {kind} ===")
    busy = [p for p in b.PORTS if b.port_busy(p)]
    if busy:
        sys.exit(f"Portas ocupadas: {busy}")
    e = b.Env(args, "clk")
    try:
        e.reference(); e.broker(); e.server("clk-1", 5555, "a")
        if not b.wait_registered("clk-1", 20):
            sys.exit("servidor nao registrou no Reference")
        print("\n[SERVIDOR]")
        probe("Servidor", b.SERVER_A, "LOGIN_REQUEST", {"bot_name": "clk"})
    finally:
        e.cleanup()
    e = b.Env(args, "clkref")
    try:
        e.reference()
        print("\n[REFERENCE]")
        probe("Reference", b.REF_ADDR, "REFERENCE_LIST_REQUEST", {})
    finally:
        e.cleanup()
    fails = [r for r in b.results if not r[2]]
    print(f"\n{len(b.results) - len(fails)} PASS, {len(fails)} FAIL")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
