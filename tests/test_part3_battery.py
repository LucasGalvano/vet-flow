"""
Bateria de testes da PARTE 3 (Reference Service, heartbeat, relogio logico,
falhas). Sobe sozinha o Reference, o broker e o(s) servidor(es), roda os
cenarios e imprime PASS/FAIL. Valida o servidor Python OU o Java.

Uso (na raiz do repositorio, com as dependencias instaladas):

  # servidor Python
  python tests/test_part3_battery.py

  # servidor Java (depois de `mvn package` em java/server)
  python tests/test_part3_battery.py --java-jar java/server/target/vetflow-server.jar

  # tambem testar o BOT_LOOP do client Java
  python tests/test_part3_battery.py --java-jar java/server/target/vetflow-server.jar \
         --java-client-jar java/client/target/vetflow-client.jar

IMPORTANTE: pare qualquer Reference/broker/servidor que esteja rodando antes
(o script usa as portas 5555, 5565, 5557, 5558 e 5559 e aborta se estiverem
ocupadas). Cada cenario usa pastas de dados temporarias, entao NAO mexe nos
seus data/*.msgpack.

Os cenarios descrevem o comportamento DESEJADO. Os marcados [BUG #n] falham
no codigo atual e devem passar depois da correcao.
"""
import argparse
import os
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import msgpack
import zmq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REF_ADDR = "tcp://localhost:5559"
SERVER_A = "tcp://localhost:5555"
SERVER_B = "tcp://localhost:5565"
PORTS = (5555, 5565, 5557, 5558, 5559)

results = []  # (cenario, check, ok, detalhe)


def record(scenario, check, ok, detail=""):
    results.append((scenario, check, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {check}" + (f"  -> {detail}" if detail else ""))


# ----------------------------------------------------------------- processos
class Proc:
    def __init__(self, name, cmd, cwd, env, logdir):
        self.name = name
        self.logpath = os.path.join(logdir, f"{name}.log")
        self._log = open(self.logpath, "wb")
        full_env = dict(os.environ)
        full_env.update(env)
        full_env["PYTHONUNBUFFERED"] = "1"
        full_env["PYTHONIOENCODING"] = "utf-8"
        self.p = subprocess.Popen(cmd, cwd=cwd, env=full_env, stdout=self._log,
                                  stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        self.started = time.time()

    def alive(self):
        return self.p.poll() is None

    def stop(self):
        if self.alive():
            if os.name == "nt":
                # No Windows o java.exe pode ser um launcher que cria a JVM como
                # processo FILHO; terminate() mataria so o launcher e deixaria a
                # JVM orfa segurando a porta. /T encerra a arvore inteira.
                subprocess.run(["taskkill", "/PID", str(self.p.pid), "/T", "/F"],
                               capture_output=True)
            else:
                self.p.terminate()
            try:
                self.p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.p.kill()
        self._log.close()

    def log(self):
        self._log.flush()
        with open(self.logpath, "rb") as f:
            return f.read().decode("utf-8", errors="replace")

    def tail(self, n=3):
        lines = [l for l in self.log().splitlines() if l.strip()]
        return " | ".join(lines[-n:])[:300]


class Env:
    """Contexto de um cenario: pasta de dados propria e processos a limpar."""

    def __init__(self, args, name):
        self.args = args
        ensure_ports_free(f"antes do cenario {name}")
        self.dir = tempfile.mkdtemp(prefix=f"vf-{name}-")
        self.procs = []

    def data_env(self, tag):
        d = os.path.join(self.dir, tag)
        os.makedirs(d, exist_ok=True)
        return {
            "LOGINS_PERSISTENCE_PATH": os.path.join(d, "logins.msgpack"),
            "CHANNELS_PERSISTENCE_PATH": os.path.join(d, "channels.msgpack"),
            "MESSAGES_PERSISTENCE_PATH": os.path.join(d, "messages.msgpack"),
        }

    def _add(self, proc):
        self.procs.append(proc)
        return proc

    def reference(self, timeout=None, tag="ref"):
        env = {}
        if timeout is not None:
            env["REFERENCE_TIMEOUT_SECONDS"] = str(timeout)
        return self._add(Proc(tag, [sys.executable, "reference.py"],
                              os.path.join(ROOT, "reference"), env, self.dir))

    def broker(self):
        return self._add(Proc("broker", [sys.executable, "broker.py"],
                              os.path.join(ROOT, "broker"), {}, self.dir))

    def server(self, name, bind_port, tag):
        env = self.data_env(tag)
        env.update({"SERVER_NAME": name, "SERVER_BIND_ADDRESS": f"tcp://*:{bind_port}"})
        if self.args.java_jar:
            cmd = ["java", "-jar", self.args.java_jar]
            cwd = ROOT
        elif self.args.server_cmd:
            cmd = shlex.split(self.args.server_cmd, posix=(os.name != "nt"))
            cwd = ROOT
        else:
            cmd = [sys.executable, "server.py"]
            cwd = os.path.join(ROOT, "python", "server")
        return self._add(Proc(tag, cmd, cwd, env, self.dir))

    def cleanup(self):
        for p in reversed(self.procs):
            p.stop()
        shutil.rmtree(self.dir, ignore_errors=True)


# --------------------------------------------------------------------- rede
def envelope(msg_type, payload, clock=1):
    return {"type": msg_type, "sender_id": "battery", "sender_lang": "python",
            "timestamp": int(time.time() * 1000), "logical_clock": clock, "payload": payload}


def request(addr, msg_type, payload, timeout_ms=15000, clock=1):
    ctx = zmq.Context()
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, timeout_ms)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(addr)
    try:
        s.send(msgpack.packb(envelope(msg_type, payload, clock), use_bin_type=True))
        return msgpack.unpackb(s.recv(), raw=False)
    finally:
        s.close()
        ctx.term()


def login(addr=SERVER_A, clock=1):
    return request(addr, "LOGIN_REQUEST", {"bot_name": "battery"}, clock=clock)


def list_servers():
    try:
        r = request(REF_ADDR, "REFERENCE_LIST_REQUEST", {}, timeout_ms=3000)
        return {s["name"]: s.get("rank") for s in r["payload"]["servers"]}
    except Exception:
        return None  # Reference fora do ar


def wait_registered(name, seconds):
    end = time.time() + seconds
    while time.time() < end:
        s = list_servers()
        if s and name in s:
            return s
        time.sleep(0.5)
    return None


def port_busy(port):
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def ensure_ports_free(when, seconds=10):
    """Espera as portas liberarem; se alguma continuar ocupada, ABORTA com uma
    mensagem clara. Sem isso, um processo orfao de um cenario anterior faz o
    proximo servidor falhar no bind e gera falhas que parecem bugs do projeto."""
    end = time.time() + seconds
    busy = [p for p in PORTS if port_busy(p)]
    while busy and time.time() < end:
        time.sleep(0.5)
        busy = [p for p in PORTS if port_busy(p)]
    if busy:
        print(f"\n[INFRA] Portas {busy} continuam ocupadas {when}.")
        print("        Provavel processo orfao de um cenario anterior (ou outro programa).")
        print("        Os resultados a partir daqui NAO seriam confiaveis, entao a bateria foi interrompida.")
        print("        PowerShell: Get-Process java,python | Select Id,ProcessName,StartTime ;")
        print("                    netstat -ano | findstr \":5555\"   e depois Stop-Process -Id <Id> -Force")
        sys.exit(2)


def count(text, needle):
    return sum(1 for l in text.splitlines() if l.startswith(needle))


# ----------------------------------------------------------------- cenarios
def s1_caminho_feliz(args):
    print("\n[S1] Registro, rank, heartbeat a cada 10 mensagens e relogio logico")
    e = Env(args, "s1")
    try:
        e.reference(); e.broker(); srv = e.server("battery-1", 5555, "a")
        reg = wait_registered("battery-1", 20)
        record("S1", "servidor se registra no Reference e recebe rank", reg is not None,
               f"lista={reg}" if reg else srv.tail())
        if reg is None:
            return
        record("S1", "rank e um inteiro >= 1", isinstance(reg["battery-1"], int) and reg["battery-1"] >= 1,
               f"rank={reg['battery-1']}")
        clocks = []
        for _ in range(25):
            clocks.append(login()["logical_clock"])
        record("S1", "logical_clock das respostas cresce estritamente (25 msgs)",
               all(b > a for a, b in zip(clocks, clocks[1:])), f"{clocks[0]}..{clocks[-1]}")
        time.sleep(0.5)
        hb = count(srv.log(), "[HEARTBEAT]")
        record("S1", "25 mensagens geram exatamente 2 heartbeats (10a e 20a)", hb == 2, f"heartbeats={hb}")
        record("S1", "servidor segue vivo", srv.alive())
    finally:
        e.cleanup()


def s2_dois_servidores(args):
    print("\n[S2] Dois servidores registrados, ranks distintos")
    e = Env(args, "s2")
    try:
        e.reference(); e.broker(); a = e.server("battery-1", 5555, "a"); b = e.server("battery-2", 5565, "b")
        ok1 = wait_registered("battery-1", 20); ok2 = wait_registered("battery-2", 20)
        lst = list_servers() or {}
        record("S2", "os dois aparecem na lista", "battery-1" in lst and "battery-2" in lst, f"lista={lst}")
        record("S2", "ranks distintos", len(set(lst.values())) == len(lst) and len(lst) == 2, f"lista={lst}")
    finally:
        e.cleanup()


def s3_partida_sem_reference(args):
    print("\n[S3] [BUG #2] Servidor sobe com o Reference FORA DO AR")
    e = Env(args, "s3")
    try:
        srv = e.server("battery-1", 5555, "a")
        time.sleep(9)
        alive = srv.alive()
        record("S3", "servidor continua vivo apos 9s sem Reference [BUG #2]", alive,
               "" if alive else f"encerrou (codigo {srv.p.returncode}): {srv.tail(2)}")
        if alive:
            e.reference()
            reg = wait_registered("battery-1", 20)
            record("S3", "registra-se quando o Reference aparece", reg is not None, f"lista={reg}")
    finally:
        e.cleanup()


def s4_reference_cai_e_volta(args):
    print("\n[S4] [BUG #1/#3] Reference cai no meio da operacao e volta")
    e = Env(args, "s4")
    try:
        ref1 = e.reference(timeout=3, tag="ref1"); e.broker()
        srv = e.server("battery-1", 5555, "a")
        record("S4", "registro inicial", wait_registered("battery-1", 20) is not None, srv.tail(2))
        ref1.stop()
        time.sleep(0.5)
        t0 = None
        for i in range(1, 11):          # a 10a mensagem dispara o heartbeat
            if i == 10:
                t0 = time.time()
            login()
        blocked = time.time() - t0
        record("S4", "requisicao que dispara o heartbeat NAO trava o servidor (< 2s) [BUG #3]",
               blocked < 2.0, f"demorou {blocked:.2f}s")
        e.reference(timeout=3, tag="ref2")
        time.sleep(4.5)                  # deixa expirar o que ficou enfileirado
        for _ in range(30):              # 3 novas tentativas de heartbeat
            login()
        time.sleep(0.5)
        lst = list_servers() or {}
        record("S4", "servidor volta a ser listado depois que o Reference volta [BUG #1]",
               "battery-1" in lst, f"lista={lst} | {srv.tail(1)}")
        record("S4", "servidor continua atendendo clients", srv.alive())
    finally:
        e.cleanup()


def s5_contrato_publish(args):
    print("\n[S5] Regressao: contrato do PUBLISH_REQUEST (Partes 1 e 2)")
    script = os.path.join(ROOT, "tests", "test_publish_validation.py")
    if not os.path.exists(script):
        print("  [SKIP] tests/test_publish_validation.py nao encontrado")
        return
    e = Env(args, "s5")
    try:
        e.reference(); e.broker(); srv = e.server("battery-1", 5555, "a")
        wait_registered("battery-1", 20)
        msgs = e.data_env("a")["MESSAGES_PERSISTENCE_PATH"]
        r = subprocess.run([sys.executable, script, "--server", SERVER_A, "--broker",
                            "tcp://localhost:5558", "--messages-file", msgs],
                           capture_output=True, text=True, timeout=120)
        last = [l for l in r.stdout.splitlines() if l.strip()][-1:] or [""]
        record("S5", "test_publish_validation.py passa por inteiro", r.returncode == 0, last[0])
    finally:
        e.cleanup()


def s6_bot_loop(args, label, cmd, cwd):
    print(f"\n[S6] BOT_LOOP do client {label}")
    e = Env(args, "s6" + label)
    try:
        e.reference(); e.broker(); srv = e.server("battery-1", 5555, "a")
        wait_registered("battery-1", 20)
        env = {"ACTION": "BOT_LOOP", "BOT_NAME": f"bot-{label}", "BOT_DELAY_SECONDS": "0.3"}
        bot = e._add(Proc(f"bot-{label}", cmd, cwd, env, e.dir))
        time.sleep(14)
        record("S6", f"bot {label} segue rodando apos 14s", bot.alive(), "" if bot.alive() else bot.tail(3))
        try:
            ch = request(SERVER_A, "CHANNEL_LIST_REQUEST", {})["payload"]["channels"]
        except Exception as ex:
            ch = []
        record("S6", "criou ate 5 canais", len(ch) >= 5, f"canais={len(ch)}")
        pubs = count(srv.log(), "[PUBLISH]")
        record("S6", "publicou varias mensagens", pubs >= 8, f"publicacoes no servidor={pubs}")
        record("S6", "servidor segue vivo", srv.alive())
    finally:
        e.cleanup()


# --------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--java-jar", help="jar do servidor Java (usa o Java em vez do Python)")
    ap.add_argument("--java-client-jar", help="jar do client Java (testa tambem o BOT_LOOP Java)")
    ap.add_argument("--server-cmd", help="comando completo para subir o servidor (uso avancado)")
    ap.add_argument("--only", help="rodar so alguns cenarios, ex.: S1,S4")
    args = ap.parse_args()
    if args.java_jar:
        args.java_jar = os.path.abspath(args.java_jar)
        if not os.path.exists(args.java_jar):
            sys.exit(f"jar nao encontrado: {args.java_jar}  (rode `mvn package` em java/server)")
    busy = [p for p in PORTS if port_busy(p)]
    if busy:
        sys.exit(f"Portas ocupadas: {busy}. Pare Reference/broker/servidores antes de rodar a bateria.")

    kind = "JAVA" if (args.java_jar or args.server_cmd) else "PYTHON"
    print(f"=== Bateria da Parte 3 | servidor: {kind} ===")
    only = set(args.only.split(",")) if args.only else None
    plan = [("S1", s1_caminho_feliz), ("S2", s2_dois_servidores), ("S3", s3_partida_sem_reference),
            ("S4", s4_reference_cai_e_volta), ("S5", s5_contrato_publish)]
    for key, fn in plan:
        if only and key not in only:
            continue
        fn(args)
    if not only or "S6" in only:
        s6_bot_loop(args, "python", [sys.executable, "client.py"], os.path.join(ROOT, "python", "client"))
        if args.java_client_jar:
            s6_bot_loop(args, "java", ["java", "-jar", os.path.abspath(args.java_client_jar)], ROOT)

    print("\n=================== RESUMO ===================")
    fails = [r for r in results if not r[2]]
    for sc, check, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {sc}: {check}")
    print(f"\n{len(results) - len(fails)} PASS, {len(fails)} FAIL")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()