"""
Conformidade da PARTE 3 com o ENUNCIADO, requisito por requisito (valores
EXATOS, nao so "cresce"). Complementa tests/test_part3_battery.py, que cobre
resiliencia. Precisa de tests/test_part3_battery.py na mesma pasta.

  python tests/test_part3_enunciado.py                      # servidor Python
  python tests/test_part3_enunciado.py --java-jar java/server/target/vetflow-server.jar

Pare Reference/broker/servidores antes (usa as portas 5555, 5557-5559).
Cada item cita o trecho do enunciado que verifica.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import msgpack
import zmq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_part3_battery as b  # noqa: E402

ROOT = b.ROOT
results = []


def rec(item, check, ok, detail=""):
    results.append((item, check, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {check}" + (f"  -> {detail}" if detail else ""))


def raw_request(addr, payload_bytes):
    """Envia bytes crus (para mensagens malformadas) e decodifica a resposta."""
    ctx = zmq.Context()
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 8000)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(addr)
    try:
        s.send(payload_bytes)
        return msgpack.unpackb(s.recv(), raw=False)
    finally:
        s.close()
        ctx.term()


def has_clock_and_timestamp(r):
    return (isinstance(r.get("logical_clock"), int) and not isinstance(r.get("logical_clock"), bool)
            and r["logical_clock"] >= 1 and isinstance(r.get("timestamp"), int))


def ref(msg_type, payload=None, clock=1):
    return b.request(b.REF_ADDR, msg_type, payload or {}, timeout_ms=5000, clock=clock)


def reg(name, clock=1):
    return ref("REFERENCE_REGISTER_REQUEST", {"server_name": name}, clock)


def names():
    return [s["name"] for s in ref("REFERENCE_LIST_REQUEST")["payload"]["servers"]]


# ---------------------------------------------------------------- Reference
def part_reference(args):
    print("\n[R] Servico de REFERENCIA (enunciado: 'Sincronizacao do relogio dos servidores')")
    e = b.Env(args, "enR")
    try:
        e.reference(timeout=2)
        time.sleep(1.2)
        r = ref("REFERENCE_LIST_REQUEST", clock=0)
        rec("Relogio 1", "Reference novo: 1a resposta a uma msg com contador 0 tem contador 1 "
            "(incrementa ANTES de enviar)", r["logical_clock"] == 1, f"contador={r['logical_clock']}")
        r = ref("REFERENCE_LIST_REQUEST", clock=1000)
        rec("Relogio 2", "recebe contador 1000 -> adota o maximo (1000) e responde 1001",
            r["logical_clock"] == 1001, f"contador={r['logical_clock']}")
        r = ref("REFERENCE_LIST_REQUEST", clock=5)
        rec("Relogio 2", "recebe contador 5 (menor) -> mantem o seu (1001) e responde 1002",
            r["logical_clock"] == 1002, f"contador={r['logical_clock']}")

        # Toda mensagem leva o contador + timestamp
        probes = {
            "REGISTER": reg("probe"),
            "HEARTBEAT": ref("REFERENCE_HEARTBEAT_REQUEST", {"server_name": "probe"}),
            "LIST": ref("REFERENCE_LIST_REQUEST"),
            "tipo desconhecido": ref("FOO_BAR"),
            "bytes malformados": raw_request(b.REF_ADDR, b"\xff\xff\xff\xff"),
        }
        bad = [k for k, v in probes.items() if not has_clock_and_timestamp(v)]
        rec("Relogio (todas as msgs)", "respostas do Reference (5 tipos) trazem logical_clock e timestamp",
            not bad, f"sem contador: {bad}" if bad else "")

        # Ref 2: nome sem repeticao
        r1, r2 = reg("dup"), reg("dup")
        rec("Ref 2", "mesmo nome registrado 2x devolve o MESMO rank",
            r1["payload"].get("rank") == r2["payload"].get("rank"),
            f"ranks {r1['payload'].get('rank')} e {r2['payload'].get('rank')}")
        rec("Ref 2", "e aparece UMA vez na lista (sem repeticoes de nome)", names().count("dup") == 1,
            f"lista={names()}")

        # Ref 3/4: lista so tem disponiveis; heartbeat mantem; ausencia remove
        reg("hb-keep"); reg("hb-drop")
        for _ in range(5):
            time.sleep(0.9)
            ref("REFERENCE_HEARTBEAT_REQUEST", {"server_name": "hb-keep"})
        lst = names()
        rec("Ref 4", "com heartbeat o servidor PERMANECE na lista", "hb-keep" in lst, f"lista={lst}")
        rec("Ref 3/4", "sem heartbeat o servidor e REMOVIDO da lista de disponiveis (timeout 2s no teste)",
            "hb-drop" not in lst, f"lista={lst}")

        # Ref 5: hora correta no reply
        r = ref("REFERENCE_HEARTBEAT_REQUEST", {"server_name": "hb-keep"})
        t = r["payload"].get("reference_time_ms")
        rec("Ref 5", "o reply do heartbeat traz a HORA do Reference (reference_time_ms ~ agora)",
            isinstance(t, int) and abs(t - time.time() * 1000) < 2000,
            f"diferenca={None if not isinstance(t, int) else int(t - time.time() * 1000)}ms")
    finally:
        e.cleanup()


# ----------------------------------------------------------------- Servidor
def part_server_clock(args):
    print("\n[S] SERVIDOR: relogio logico e mensagens")
    e = b.Env(args, "enS")
    try:
        e.reference(); e.broker(); srv = e.server("conf-1", 5555, "a")
        if not b.wait_registered("conf-1", 20):
            rec("S", "servidor registrou", False, srv.tail()); return
        r = b.request(b.SERVER_A, "LOGIN_REQUEST", {"bot_name": "t"}, clock=1000)
        rec("Relogio 2", "servidor recebe contador 1000 -> responde exatamente 1001",
            r["logical_clock"] == 1001, f"contador={r['logical_clock']}")
        r = b.request(b.SERVER_A, "LOGIN_REQUEST", {"bot_name": "t"}, clock=5)
        rec("Relogio 2", "recebe contador 5 (menor) -> mantem o seu e responde 1002",
            r["logical_clock"] == 1002, f"contador={r['logical_clock']}")

        # Toda mensagem leva o contador + timestamp
        b.request(b.SERVER_A, "CHANNEL_CREATE_REQUEST", {"channel_name": "c1", "bot_name": "t"})
        probes = {
            "LOGIN": b.request(b.SERVER_A, "LOGIN_REQUEST", {"bot_name": "t"}),
            "CHANNEL_CREATE": b.request(b.SERVER_A, "CHANNEL_CREATE_REQUEST", {"channel_name": "c2", "bot_name": "t"}),
            "CHANNEL_LIST": b.request(b.SERVER_A, "CHANNEL_LIST_REQUEST", {}),
            "PUBLISH": b.request(b.SERVER_A, "PUBLISH_REQUEST", {"channel_name": "c1", "message": "m"}),
            "PUBLISH invalido": b.request(b.SERVER_A, "PUBLISH_REQUEST", {"channel_name": "c1", "message": 5}),
            "tipo desconhecido": b.request(b.SERVER_A, "FOO_BAR", {}),
            "bytes malformados": raw_request(b.SERVER_A, b"\xff\xff\xff\xff"),
        }
        bad = [k for k, v in probes.items() if not has_clock_and_timestamp(v)]
        rec("Relogio (todas as msgs)", "respostas do servidor (7 tipos, inclusive erros) trazem logical_clock e timestamp",
            not bad, f"sem contador: {bad}" if bad else "")
    finally:
        e.cleanup()


def part_server_pubsub(args):
    print("\n[P] SERVIDOR: mensagem publicada no broker (Pub/Sub) tambem leva o contador")
    e = b.Env(args, "enP")
    try:
        e.reference(); e.broker(); srv = e.server("conf-1", 5555, "a")
        b.wait_registered("conf-1", 20)
        b.request(b.SERVER_A, "CHANNEL_CREATE_REQUEST", {"channel_name": "pub", "bot_name": "t"})
        ctx = zmq.Context(); sub = ctx.socket(zmq.SUB)
        sub.connect("tcp://localhost:5558"); sub.setsockopt(zmq.SUBSCRIBE, b"pub"); sub.setsockopt(zmq.RCVTIMEO, 4000)
        time.sleep(1.5)
        resp = b.request(b.SERVER_A, "PUBLISH_REQUEST", {"channel_name": "pub", "message": "x"})
        try:
            topic, body = sub.recv_multipart()
            env = msgpack.unpackb(body, raw=False)
            rec("Relogio (todas as msgs)", "CHANNEL_MESSAGE (servidor -> broker -> bots) traz logical_clock e timestamp",
                has_clock_and_timestamp(env), f"contador={env.get('logical_clock')}")
            rec("Relogio 1", "cada envio incrementa: a mensagem publicada (n) vem ANTES da resposta (n+1)",
                resp["logical_clock"] == env["logical_clock"] + 1,
                f"publicada={env['logical_clock']} resposta={resp['logical_clock']}")
        except zmq.error.Again:
            rec("Relogio (todas as msgs)", "CHANNEL_MESSAGE recebida do broker", False, "nada chegou")
        sub.close(); ctx.term()
    finally:
        e.cleanup()


def part_server_adopts_reference_clock(args):
    print("\n[A] SERVIDOR adota o contador do Reference ao receber a resposta (regra do maximo)")
    e = b.Env(args, "enA")
    try:
        e.reference(); e.broker()
        time.sleep(1.0)
        ref("REFERENCE_LIST_REQUEST", clock=7000)       # eleva o contador do Reference
        srv = e.server("conf-1", 5555, "a")
        b.wait_registered("conf-1", 20)
        r = b.request(b.SERVER_A, "LOGIN_REQUEST", {"bot_name": "t"}, clock=1)
        rec("Relogio 2", "contador do Reference (>7000) e absorvido pelo servidor: resposta ao client > 7000",
            r["logical_clock"] > 7000, f"contador={r['logical_clock']}")
    finally:
        e.cleanup()


def part_heartbeat_counts(args):
    print("\n[H] HEARTBEAT 'a cada 10 mensagens de clientes recebidas'")
    e = b.Env(args, "enH")
    try:
        e.reference(); e.broker(); srv = e.server("conf-1", 5555, "a")
        b.wait_registered("conf-1", 20)

        def beats():
            time.sleep(0.4)
            return b.count(srv.log(), "[HEARTBEAT]")
        mixed = ([lambda: b.login()] * 3
                 + [lambda: b.request(b.SERVER_A, "CHANNEL_LIST_REQUEST", {})] * 2
                 + [lambda: b.request(b.SERVER_A, "FOO_BAR", {})] * 2
                 + [lambda: raw_request(b.SERVER_A, b"\xff\xff")] * 2
                 + [lambda: b.request(b.SERVER_A, "PUBLISH_REQUEST", {"channel_name": "nao-existe", "message": "x"})])
        for f in mixed[:9]:
            f()
        rec("Ref 4", "9 mensagens (validas e invalidas): nenhum heartbeat ainda", beats() == 0, f"heartbeats={beats()}")
        mixed[9]()
        rec("Ref 4", "a 10a mensagem de cliente dispara exatamente 1 heartbeat (conta TODAS as recebidas)",
            beats() == 1, f"heartbeats={beats()}")
        for _ in range(9):
            b.login()
        rec("Ref 4", "mais 9 mensagens: continua 1", beats() == 1, f"heartbeats={beats()}")
        b.login()
        rec("Ref 4", "a 20a dispara o 2o heartbeat", beats() == 2, f"heartbeats={beats()}")
    finally:
        e.cleanup()


def part_clock_sync(args):
    print("\n[T] Ref 5: servidor ATUALIZA seu relogio com a hora do Reference (Reference adiantado 5 s)")
    e = b.Env(args, "enT")
    skew = tempfile.mkdtemp(prefix="vf-skew-")
    try:
        for fn in ("reference.py", "registry.py"):
            src = open(os.path.join(ROOT, "reference", fn), encoding="utf-8").read()
            open(os.path.join(skew, fn), "w", encoding="utf-8").write(src.replace("time.time()", "(time.time() + 5)"))
        e._add(b.Proc("ref-skew", [sys.executable, "reference.py"], skew, {}, e.dir))
        e.broker()
        srv = e.server("conf-1", 5555, "a")
        b.wait_registered("conf-1", 20)
        r = b.login()
        d = r["timestamp"] - time.time() * 1000
        rec("Ref 5", "apos o registro, timestamps do servidor ~ +5000 ms em relacao ao relogio local",
            4000 < d < 6000, f"diferenca={d:.0f}ms")
        for _ in range(9):
            b.login()
        time.sleep(0.5)
        d = b.login()["timestamp"] - time.time() * 1000
        rec("Ref 5", "apos um heartbeat (hora pedida de novo) continua ~ +5000 ms",
            4000 < d < 6000, f"diferenca={d:.0f}ms")
    finally:
        shutil.rmtree(skew, ignore_errors=True)
        e.cleanup()



# ------------------------------------------------------------------ Clients
def part_client_clock(args, label, cmd, cwd):
    """Servidor FALSO que responde com contadores altos: mostra se o client
    incrementa antes de enviar, adota o maximo e absorve o contador do Pub/Sub."""
    print(f"\n[C] CLIENT {label}: relogio logico do bot (enunciado: 'implementado tanto no cliente/bot como no servidor')")
    e = b.Env(args, "enC" + label)
    ctx = zmq.Context()
    stop = threading.Event()
    seen, replies = [], []
    rep = ctx.socket(zmq.REP)
    rep.setsockopt(zmq.RCVTIMEO, 300)
    rep.setsockopt(zmq.LINGER, 0)
    rep.bind("tcp://*:5555")

    def serve():
        n = 0
        while not stop.is_set():
            try:
                req = msgpack.unpackb(rep.recv(), raw=False)
            except zmq.error.Again:
                continue
            n += 1
            clock = 1000 + 100 * n
            seen.append(req.get("logical_clock"))
            replies.append(clock)
            t = req["type"]
            payload = {"status": "OK"}
            if t == "CHANNEL_LIST_REQUEST":
                payload["channels"] = ["a", "b", "c", "d", "e"]
            rep.send(msgpack.packb({"type": t.replace("REQUEST", "RESPONSE"), "sender_id": "fake",
                                    "sender_lang": "python", "timestamp": int(time.time() * 1000),
                                    "logical_clock": clock, "payload": payload}, use_bin_type=True))

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    try:
        e.broker()
        time.sleep(1.0)
        env = {"ACTION": "BOT_LOOP", "BOT_NAME": f"bot-{label}", "BOT_DELAY_SECONDS": "0.2"}
        e._add(b.Proc(f"client-{label}", cmd, cwd, env, e.dir))
        end = time.time() + 25            # espera requisicoes suficientes (JVM e mais lenta)
        while len(seen) < 10 and time.time() < end:
            time.sleep(0.2)
        n_before = len(seen)
        rec("Relogio 1", f"1a mensagem do client carrega contador 1 (contador inicia em 0 e incrementa ANTES de enviar)",
            n_before > 0 and seen[0] == 1, f"1o contador enviado={seen[0] if seen else None}")
        pairs = [(replies[i], seen[i + 1]) for i in range(min(len(replies), len(seen)) - 1)]
        wrong = [(r, c) for r, c in pairs if c != r + 1]
        rec("Relogio 2", "apos cada resposta (contador alto), o proximo envio e EXATAMENTE max(proprio, recebido)+1",
            n_before >= 6 and not wrong,
            f"{n_before} requisicoes, {len(pairs)} pares verificados; divergencias={wrong[:3]}")

        # Pub/Sub: o bot absorve o contador de uma CHANNEL_MESSAGE
        pub = ctx.socket(zmq.PUB)
        pub.connect("tcp://localhost:5557")
        time.sleep(1.0)
        deadline = time.time() + 5
        while time.time() < deadline:
            for ch in "abcde":
                msg = {"type": "CHANNEL_MESSAGE", "sender_id": "fake-pub", "sender_lang": "python",
                       "timestamp": int(time.time() * 1000), "logical_clock": 90000,
                       "payload": {"channel_name": ch, "message": "x"}}
                pub.send_multipart([ch.encode(), msgpack.packb(msg, use_bin_type=True)])
            time.sleep(0.3)
        pub.close()
        rec("Relogio 2 (Pub/Sub)", "ao receber CHANNEL_MESSAGE com contador 90000, o proximo envio do bot e 90001",
            90001 in seen, f"maior contador enviado={max(c for c in seen if isinstance(c, int))}")
    finally:
        stop.set()
        th.join(timeout=2)
        rep.close()
        ctx.term()
        e.cleanup()

# ------------------------------------------------------------ Entrega (estatico)
def part_delivery():
    print("\n[E] ENTREGA (verificacao estatica do repositorio)")
    import yaml
    sv = yaml.safe_load(open(os.path.join(ROOT, "docker-compose.yml"), encoding="utf-8"))["services"]
    ref_names = [n for n in sv if "reference" in n]
    rec("Entrega", "docker-compose.yml tem o servico de referencia (build ./reference)",
        bool(ref_names) and str(sv[ref_names[0]].get("build", "")).strip("./") == "reference", f"servicos={ref_names}")
    servers = [n for n in sv if n.endswith("-server")]
    rec("Entrega", "os servidores dependem do servico de referencia no compose",
        all(ref_names and ref_names[0] in (sv[s].get("depends_on") or {}) for s in servers), f"servidores={servers}")
    bots = [n for n in sv if n.startswith("bot-")]
    leak = [n for n in bots if "REFERENCE" in str(sv[n].get("environment", "")) or ref_names[0] in (sv[n].get("depends_on") or {})]
    rec("Ref (intro)", "bots/clients NAO falam com o Reference ('nao recebera mensagens dos clientes')",
        not leak, f"bots que usam o Reference: {leak}" if leak else "")
    src = ""
    for p in (os.path.join(ROOT, "python", "client", "client.py"),
              os.path.join(ROOT, "java", "client", "src", "main", "java", "com", "vetflow", "client", "Client.java")):
        src += open(p, encoding="utf-8").read()
    rec("Ref (intro)", "codigo dos clients nao referencia o Reference Service", "REFERENCE" not in src.upper().replace("REFERENCE_TIME", ""))
    try:
        base = next(x for x in ("origin/main", "main") if subprocess.run(
            ["git", "rev-parse", "--verify", "-q", x], cwd=ROOT, capture_output=True).returncode == 0)
        diff = subprocess.run(["git", "diff", "--stat", base, "--", "broker"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
        rec("Entrega", f"broker/proxy SEM alteracao em relacao a {base} ('broker e proxy sem atualizacao')",
            diff == "", diff.replace("\n", " | ")[:160])
    except StopIteration:
        print("  [SKIP] sem git/main para comparar o broker")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--java-jar"); ap.add_argument("--server-cmd"); ap.add_argument("--only")
    ap.add_argument("--java-client-jar"); ap.add_argument("--java-client-cmd")
    args = ap.parse_args()
    if args.java_jar:
        args.java_jar = os.path.abspath(args.java_jar)
    busy = [p for p in b.PORTS if b.port_busy(p)]
    if busy:
        sys.exit(f"Portas ocupadas: {busy}. Pare Reference/broker/servidores antes.")
    kind = "JAVA" if (args.java_jar or args.server_cmd) else "PYTHON"
    print(f"=== Conformidade da Parte 3 com o enunciado | servidor: {kind} ===")
    only = set(args.only.split(",")) if args.only else None
    plan = [("R", part_reference), ("S", part_server_clock), ("P", part_server_pubsub),
            ("A", part_server_adopts_reference_clock), ("H", part_heartbeat_counts), ("T", part_clock_sync)]
    for key, fn in plan:
        if not only or key in only:
            fn(args)
    if not only or "C" in only:
        part_client_clock(args, "python", [sys.executable, "client.py"], os.path.join(ROOT, "python", "client"))
        jc = None
        if args.java_client_jar:
            jc = ["java", "-jar", os.path.abspath(args.java_client_jar)]
        elif args.java_client_cmd:
            import shlex
            jc = shlex.split(args.java_client_cmd, posix=(os.name != "nt"))
        if jc:
            part_client_clock(args, "java", jc, ROOT)
    if not only or "E" in only:
        part_delivery()
    fails = [r for r in results if not r[2]]
    print("\n=================== RESUMO ===================")
    for item, check, ok in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {item}: {check}")
    print(f"\n{len(results) - len(fails)} PASS, {len(fails)} FAIL")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
