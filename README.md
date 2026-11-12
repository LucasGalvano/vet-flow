# vet_flow — Sistema Distribuído de Mensagens (Clínica Veterinária)

> **Status do código:** Partes 1 a 4 implementadas (Python e Java, com
> interoperabilidade entre as linguagens).
> Parte 1: login, criação e listagem de canais, com persistência em disco.
> Parte 2: Pub/Sub via broker (`XSUB` 5557 / `XPUB` 5558), com persistência das
> mensagens publicadas e bots padronizados (`ACTION=BOT_LOOP`).
> Parte 3: relógios lógicos, serviço de referência (rank, lista de servidores e
> heartbeat) e broker REQ/REP com round-robin entre os servidores.
> Parte 4: eleição de coordenador (algoritmo do valentão) e sincronização do
> relógio físico dos servidores com o coordenador.
> A replicação de dados (Parte 5) ainda **não** está implementada.

## Objetivo

Projeto acadêmico da disciplina de Sistemas Distribuídos: um sistema de
troca de mensagens usando ZeroMQ, com múltiplos clientes (bots) e múltiplos
servidores, aplicado ao domínio de uma clínica veterinária (registros de
validade de produtos, limpeza, avisos, histórico de operações).

O sistema é implementado em duas linguagens — **Python** e **Java** —, cada
uma com uma implementação completa (client + server), como réplicas
heterogêneas que falam o mesmo protocolo.

## Arquitetura geral (visão do projeto completo)

> Esta seção descreve a arquitetura **pretendida** para o projeto completo,
> não o que já está implementado. Veja "Status atual" acima para saber o
> que já funciona.

O projeto terá 5 partes: (1) login/canais/persistência, (2) Pub/Sub via
broker, (3) relógios lógicos + heartbeat + Reference Service, (4) eleição
de coordenador + algoritmo de Berkeley, (5) replicação entre réplicas.


## O que já está implementado (Parte 1 — completa)

- `python/server` e `java/server`: servidor ZeroMQ (`REP`) que trata
  `LOGIN_REQUEST`, `CHANNEL_CREATE_REQUEST` e `CHANNEL_LIST_REQUEST`, com
  persistência em disco (MessagePack) para logins e canais.
- `python/client` e `java/client`: bot ZeroMQ (`REQ`) que envia uma dessas
  requisições por execução via `ACTION`, além do modo contínuo `BOT_LOOP`.
- Nome de canal duplicado é rejeitado com erro (ambas as linguagens).
- **Interoperabilidade Python ↔ Java confirmada** nos dois sentidos, para
  os três tipos de mensagem — incluindo o caso mais importante: um canal
  criado por um bot Java, persistido por um servidor Python, lido de
  volta corretamente por um client Python.

## O que já está implementado (Parte 2 — Pub/Sub)

- `broker/`: proxy `XSUB` (5557) / `XPUB` (5558) em Python, sem lógica de
  negócio (`zmq.proxy`).
- Servidores (Python e Java): `PUBLISH_REQUEST` via REQ/REP → valida que o
  canal existe → **persiste** em `messages.msgpack` → **só então publica**
  no broker (durabilidade antes de visibilidade).
- Clients (Python e Java): `ACTION=PUBLISH` e `ACTION=SUBSCRIBE`
  (`SUB` direto no broker, escuta por `SUBSCRIBE_SECONDS` e sai).
- Framing: multipart — frame 1 = tópico (nome do canal, UTF-8, mecanismo de
  roteamento), frame 2 = envelope completo em MessagePack.
- Interoperabilidade Pub/Sub testada manualmente: Python→Python,
  Java→Java, servidor Python→subscriber Java e servidor Java→subscriber
  Python.
- Limitação conhecida: o ZeroMQ faz *prefix match* no tópico (um subscriber
  de `vac` também recebe `vacinas`).

## O que já está implementado (Parte 3 — relógios, referência e heartbeat)

- **Relógio lógico** em bots, servidores e Reference Service: o contador
  incrementa antes de cada envio e, ao receber, assume o máximo entre o valor
  local e o recebido. Todo envelope carrega `logical_clock` além do `timestamp`.
- **Reference Service** (`reference/`, porta 5559, REQ/REP MessagePack):
  atribui um rank estável por nome (`REFERENCE_REGISTER_REQUEST`), lista os
  servidores disponíveis (`REFERENCE_LIST_REQUEST`) e renova a disponibilidade a
  cada heartbeat (`REFERENCE_HEARTBEAT_REQUEST`).
- **Heartbeat:** cada servidor o envia a cada 10 mensagens de clientes recebidas
  (`HEARTBEAT_EVERY`). Sem heartbeat por 30 s o servidor sai da lista, mas mantém
  o rank ao voltar. (Na Parte 3 a resposta trazia a hora de referência; na Parte 4
  ela deixou de trazer, ver abaixo.)
- **Broker REQ/REP** (`broker-reqrep/`): `ROUTER` (5555, bots) e `DEALER` (5556,
  servidores), com round-robin entre os servidores. Ver "Brokers".
- **Logs completos** de tudo que é enviado e recebido, inclusive as trocas
  servidor-Reference e a publicação no tópico.

## O que já está implementado (Parte 4 — eleição e sincronização de relógio)

**Coordenador.** Cada servidor guarda em `coordinator` o nome do servidor eleito.
O coordenador é o servidor de **maior rank** disponível (algoritmo do valentão /
*bully*, o padrão do diagrama do enunciado).

**Eleição.** Ao iniciar (depois de receber o rank do Reference), ou quando o
coordenador não responde, o servidor:
1. consulta a lista de servidores no Reference (`REFERENCE_LIST_REQUEST`);
2. envia `ELECTION_REQUEST` **só aos de rank maior** (`tcp://<nome>:5555`);
3. se algum responde `OK`, espera o anúncio dele (`ELECTION_WAIT_SECONDS`, 5 s) e
   refaz a eleição se ele não chegar; o servidor que respondeu `OK` assume a
   eleição por conta própria;
4. se nenhum responde, torna-se coordenador e publica o nome no tópico `servers`.

Quando um servidor de rank maior volta, ele mesmo se elege ao iniciar e passa a
ser o coordenador; os demais adotam o anúncio dele.

**Anúncio.** `COORDINATOR_ANNOUNCEMENT` (`{coordinator, rank}`) publicado no tópico
`servers` do broker Pub/Sub; todo servidor assina esse tópico no `XPUB` (5558).
O coordenador **republica** o anúncio a cada 5 s (`COORDINATOR_ANNOUNCE_SECONDS`):
o Pub/Sub do ZeroMQ não garante entrega a quem acabou de assinar (*slow joiner*)
e isso também desfaz coordenadores divergentes (um anúncio de rank **menor** faz o
servidor maior disputar a coordenação). O nome `servers` é reservado: não é
possível criar um canal com ele.

**Sincronização de relógio.** A cada 15 mensagens de clientes recebidas
(`SYNC_EVERY`), e logo que passa a conhecer um novo coordenador, o servidor envia
`CLOCK_REQUEST` ao coordenador e recebe `CLOCK_RESPONSE` com a hora dele
(`time_ms`). O servidor ajusta o relógio compensando **metade do tempo de ida e
volta** (estimativa de Cristian). Se o coordenador não responde (ou responde que
não é mais o coordenador), é tratado como indisponível e uma nova eleição começa.
O coordenador é a referência de tempo e não se ajusta.

> Versão simplificada de Berkeley: conforme o diagrama do enunciado, o
> coordenador apenas **informa a hora correta**; ele não coleta nem faz a média
> dos relógios dos demais.

**Reference Service.** O heartbeat não devolve mais a hora (`reference_time_ms`);
continua dando rank, lista e heartbeat. A lista só tem nome e rank: o endereço de
um servidor é `tcp://<nome>:5555` (o nome registrado é o hostname no Compose).

**Desvio de relógio para demonstração.** No Docker todos os containers compartilham
o mesmo relógio, então a sincronização não teria o que corrigir. `CLOCK_SKEW_MS`
simula um relógio local errado: no `docker-compose.yml`, `python-server` tem
`-2000` e `java-server` `0`. Para desligar, remova a variável.

**Mensagens novas** (mesmo envelope: `type`, `sender_id`, `sender_lang`,
`timestamp`, `logical_clock`, `payload`):

| Tipo | Canal | Payload |
|---|---|---|
| `ELECTION_REQUEST` | REQ servidor→servidor (rank maior) | `{rank}` |
| `ELECTION_RESPONSE` | REP | `{status: "OK", rank}` |
| `CLOCK_REQUEST` | REQ servidor→coordenador | `{}` |
| `CLOCK_RESPONSE` | REP | `{status: "OK", time_ms}` ou `{status: "ERROR", error_msg, coordinator}` |
| `COORDINATOR_ANNOUNCEMENT` | PUB, tópico `servers` | `{coordinator, rank}` |

**Detalhes de implementação.**
- Tudo continua no loop único do `REP`, sem threads: o servidor atende os pares na
  mesma porta 5555 (direto, sem passar pelo broker REQ/REP) e lê o tópico
  `servers` sem bloquear a cada ciclo. O heartbeat e a sincronização agora rodam
  **depois** de responder o cliente.
- Um servidor só inicia trocas com servidores de rank **maior** (eleição) ou com
  o coordenador (relógio), então as esperas formam uma cadeia e não um ciclo; o
  pior caso é um atraso de até `PEER_TIMEOUT_MS` (1 s) por par que não responde.
- Mensagens `ELECTION_REQUEST` e `CLOCK_REQUEST` não contam para o heartbeat nem
  para a sincronização (só mensagens de clientes).
- Logs novos: `[ELEICAO]`, `[COORDENADOR]`, `[CLOCK-SYNC]`, `[PEER-SEND]`/`[PEER-RECV]`
  e `[PUB-SEND]`/`[PUBSUB-RECV]` do tópico `servers`.

## Estrutura de diretórios

```
projeto/
├── .gitignore
├── .env.example
├── README.md
├── docker-compose.yml
├── broker/                # proxy Pub/Sub (XSUB 5557 / XPUB 5558)
│   ├── broker.py
│   └── Dockerfile
├── broker-reqrep/         # broker REQ/REP (ROUTER 5555 / DEALER 5556)
│   ├── broker_reqrep.py
│   └── Dockerfile
├── reference/
│   ├── reference.py       # serviço de referência (Parte 3)
│   ├── registry.py        # ranks e leases de heartbeat
│   └── Dockerfile
├── tests/
│   ├── test_publish_validation.py   # contrato do PUBLISH_REQUEST (qualquer servidor)
│   ├── test_part3.py                # ranks e disponibilidade
│   └── list_servers.py              # consulta a lista de servidores no Reference
├── python/
│   ├── server/            # server.py, persistence.py, requirements.txt, Dockerfile
│   └── client/            # client.py, requirements.txt, Dockerfile
└── java/
    ├── server/            # pom.xml, Dockerfile, src/main/java/com/vetflow/server/{Server,Persistence}.java
    └── client/            # pom.xml, Dockerfile, src/main/java/com/vetflow/client/Client.java
```

## Dependências (Java)

- JDK 17+ (`java -version` para conferir)
- Maven 3.6+ (`mvn -version` para conferir)
- Dependências gerenciadas pelo Maven (baixadas do Maven Central
  automaticamente no `mvn package`):
  - [JeroMQ](https://github.com/zeromq/jeromq) — implementação ZeroMQ em Java puro
  - [jackson-dataformat-msgpack](https://github.com/msgpack/msgpack-java) — serialização MessagePack

Build:

```bash
cd java/server && mvn package   # gera target/vetflow-server.jar
cd java/client && mvn package   # gera target/vetflow-client.jar
```

## Dependências (Python)

- Python 3.10+
- [pyzmq](https://pypi.org/project/pyzmq/) — bindings ZeroMQ
- [msgpack](https://pypi.org/project/msgpack/) — serialização binária

Instalação:

```bash
cd python/server && pip install -r requirements.txt
cd python/client && pip install -r requirements.txt
```

## Variáveis de ambiente

Veja [`.env.example`](.env.example) para a lista completa. Nenhuma delas é
obrigatória — todas têm valor padrão seguro no código.

| Variável | Usado por | Padrão | Descrição |
|---|---|---|---|
| `BOT_NAME` | client | `bot-python-1` | Nome do bot que faz login / cria canais |
| `SERVER_ADDRESS` | client | `tcp://localhost:5555` | Endereço do servidor |
| `ACTION` | client | `LOGIN` | Ação a executar: `LOGIN`, `CHANNEL_CREATE`, `CHANNEL_LIST`, `PUBLISH` ou `SUBSCRIBE` |
| `CHANNEL_NAME` | client | `avisos-gerais` | Nome do canal, usado quando `ACTION=CHANNEL_CREATE` |
| `SERVER_BIND_ADDRESS` | server | `tcp://*:5555` | Endereço de bind do servidor |
| `LOGINS_PERSISTENCE_PATH` | server | `python/server/data/logins.msgpack` | Caminho do arquivo de persistência de logins |
| `CHANNELS_PERSISTENCE_PATH` | server | `python/server/data/channels.msgpack` | Caminho do arquivo de persistência de canais |
| `MESSAGES_PERSISTENCE_PATH` | server | `python/server/data/messages.msgpack` | Caminho do arquivo de persistência de mensagens publicadas |
| `BROKER_XSUB_ADDRESS` | server | `tcp://localhost:5557` | Endereço do `XSUB` do broker, onde o servidor conecta seu `PUB` |
| `BROKER_ADDRESS` | client | `tcp://localhost:5558` | Endereço do `XPUB` do broker, usado em `ACTION=SUBSCRIBE` |
| `MESSAGE` | client | `mensagem de teste` | Texto publicado em `ACTION=PUBLISH` |
| `SUBSCRIBE_SECONDS` | client | `5` | Por quanto tempo `ACTION=SUBSCRIBE` escuta antes de sair |
| `CHANNELS` | client | valor de `CHANNEL_NAME` | Lista separada por vírgulas de canais para `ACTION=SUBSCRIBE` |
| `BOT_DELAY_SECONDS` | client | `1` | Pausa entre publicações de `ACTION=BOT_LOOP` |
| `REFERENCE_ADDRESS` | server | `tcp://localhost:5559` | Endereço do serviço de referência |
| `SERVER_BROKER_ADDRESS` | server | _(vazio)_ | Backend do broker REQ/REP (ex.: `tcp://reqrep-broker:5556`). Se vazio, o servidor só faz `bind` |
| `BROKER_XPUB_ADDRESS` | server | `tcp://localhost:5558` | XPUB do broker Pub/Sub, onde o servidor assina o tópico `servers` (Parte 4) |
| `SYNC_EVERY` | server | `15` | Mensagens de clientes entre sincronizações de relógio com o coordenador |
| `PEER_PORT` | server | `5555` | Porta dos pares (endereço do par: `tcp://<nome>:PEER_PORT`) |
| `PEER_TIMEOUT_MS` | server | `1000` | Timeout das trocas entre servidores (eleição e relógio) |
| `ELECTION_WAIT_SECONDS` | server | `5` | Espera pelo anúncio depois de um servidor maior responder OK |
| `COORDINATOR_ANNOUNCE_SECONDS` | server | `5` | Intervalo da republicação do anúncio pelo coordenador |
| `CLOCK_SKEW_MS` | server | `0` | Desvio artificial do relógio local (demonstração da sincronização) |
| `SERVER_NAME` | server | nome da implementação | Nome único usado no registro e heartbeat |
| `HEARTBEAT_EVERY` | server | `10` | Mensagens de clientes entre heartbeats |
| `REFERENCE_TIMEOUT_MS` | server | `1000` | Quanto o servidor espera a resposta do Reference Service em cada troca (registro/heartbeat). **Não confundir** com `REFERENCE_TIMEOUT_SECONDS`, que é do lado do Reference |
| `REFERENCE_RETRY_INITIAL_SECONDS` | server | `1` | Intervalo inicial entre tentativas de registro quando o Reference está indisponível |
| `REFERENCE_RETRY_MAX_SECONDS` | server | `10` | Teto do backoff (o intervalo dobra a cada falha até este valor) |
| `REFERENCE_TIMEOUT_SECONDS` | reference | `30` | Tempo sem heartbeat para remover servidor da lista disponível |
| `REFERENCE_BIND_ADDRESS` | reference | `tcp://*:5559` | Endereço de bind do serviço de referência |
| `REQREP_FRONTEND_BIND` | broker-reqrep | `tcp://*:5555` | Bind do `ROUTER` (lado dos bots) |
| `REQREP_BACKEND_BIND` | broker-reqrep | `tcp://*:5556` | Bind do `DEALER` (lado dos servidores) |
| `BROKER_XSUB_BIND` / `BROKER_XPUB_BIND` | broker | `tcp://*:5557` / `tcp://*:5558` | Binds do proxy Pub/Sub |

O projeto **não** usa `python-dotenv` — as variáveis são lidas diretamente
via `os.environ.get(...)`. Para usá-las, exporte-as no shell antes de rodar
(a sintaxe varia por sistema operacional — veja abaixo).

## Como testar: Python Client ↔ Python Server

**Terminal 1 — subir o servidor:**

```bash
cd python/server
python3 server.py
```

Deve aparecer: `[SERVER] Historico carregado: N login(s) e N canal(is) ...`

**Terminal 2 — rodar o client (bot) para cada ação:**

- **Linux / macOS (bash/zsh):**
  ```bash
  cd python/client
  BOT_NAME="dra-ana-vet" ACTION=LOGIN python3 client.py
  BOT_NAME="dra-ana-vet" ACTION=CHANNEL_CREATE CHANNEL_NAME="vacinas" python3 client.py
  ACTION=CHANNEL_LIST python3 client.py
  ```

- **Windows — PowerShell:**
  ```powershell
  cd python\client
  $env:BOT_NAME="dra-ana-vet"; $env:ACTION="LOGIN"; python client.py
  $env:ACTION="CHANNEL_CREATE"; $env:CHANNEL_NAME="vacinas"; python client.py
  $env:ACTION="CHANNEL_LIST"; python client.py
  ```

  ⚠️ Lembre-se: `set VAR=valor` (sintaxe do CMD) não funciona no PowerShell
  — use sempre `$env:VAR="valor"`.

Saída esperada de `ACTION=CHANNEL_LIST`:

```
[CLIENT] Canais existentes (1): ['vacinas']
```

## Como testar: Java Client ↔ Java Server

**Terminal 1 — subir o servidor Java:**

```powershell
cd java\server
mvn package
java -jar target\vetflow-server.jar
```

Como o Python Server já pode estar rodando na porta 5555, use outra porta
para o Java enquanto testamos localmente sem Docker:

```powershell
$env:SERVER_BIND_ADDRESS="tcp://*:5556"
java -jar target\vetflow-server.jar
```

**Terminal 2 — rodar o client Java:**

```powershell
cd java\client
mvn package
$env:SERVER_ADDRESS="tcp://localhost:5556"; $env:BOT_NAME="bot-java-1"; $env:ACTION="LOGIN"
java -jar target\vetflow-client.jar
```

## Como testar: interoperabilidade Python ↔ Java

**Confirmado funcionando** para `LOGIN`, `CHANNEL_CREATE` e `CHANNEL_LIST`,
nos dois sentidos.

Com o Java Server rodando na porta 5556 (acima), rode o **client Python**
apontando para ele:

```powershell
cd python\client
$env:SERVER_ADDRESS="tcp://localhost:5556"; $env:BOT_NAME="dra-ana-vet"; $env:ACTION="LOGIN"
python client.py
```

E, com o Python Server rodando na porta 5555 (padrão), rode o **client
Java** apontando para ele:

```powershell
cd java\client
$env:SERVER_ADDRESS="tcp://localhost:5555"; $env:BOT_NAME="bot-java-1"; $env:ACTION="LOGIN"
java -jar target\vetflow-client.jar
```

## Testes

### Parte 3 — relógios, serviço de referência e heartbeat

O serviço de referência fica em `reference/` e usa `REQ/REP` MessagePack na
porta `5559`. Cada servidor se registra ao iniciar e recebe um rank estável.
Se o Reference estiver indisponível na partida, o servidor **sobe mesmo assim** e
tenta registrar-se de novo automaticamente, com backoff, sem precisar ser
reiniciado.
O tipo `REFERENCE_LIST_REQUEST` retorna os servidores disponíveis com seus
ranks. O servidor envia `REFERENCE_HEARTBEAT_REQUEST` a cada 10 mensagens de
clientes; a resposta contém a hora de referência e ajusta o deslocamento
usado nos timestamps físicos seguintes.

O serviço mantém ranks mesmo depois de um processo ficar indisponível. A
lista de disponíveis é uma lease em memória: após **30 segundos** sem
heartbeat o servidor sai da lista, mas conserva o mesmo rank ao se registrar
novamente. Esse limite pode ser alterado com `REFERENCE_TIMEOUT_SECONDS`.

**Ranks em memória.** Os ranks vivem apenas na memória do Reference: são estáveis
enquanto ele está no ar, mas se o Reference reiniciar a numeração recomeça pelo
primeiro servidor que se registrar. Isso é aceitável nesta parte do projeto.

**Relógio dos servidores.** Na Parte 3 o servidor ajustava o relógio pela hora devolvida no heartbeat do Reference (sem compensar a latência). Na Parte 4 isso foi substituído pela sincronização com o coordenador eleito (ver acima).

**Resiliência frente a um Reference indisponível.** O servidor não fica preso a
ele: a troca com o Reference tem timeout curto (`REFERENCE_TIMEOUT_MS`, padrão
1000 ms); depois de uma falha de comunicação o socket `REQ` é **recriado** (um
`REQ` que estoura o timeout fica inutilizável); uma falha de heartbeat volta o
servidor ao estado "não registrado" e as tentativas de registro seguem com
backoff (`REFERENCE_RETRY_INITIAL_SECONDS` → `REFERENCE_RETRY_MAX_SECONDS`).
Isso roda no mesmo loop único do `REP` (sem threads): o `recv` acorda a cada
500 ms para tentar o registro mesmo sem tráfego.

Limitações conhecidas: (1) uma troca com o Reference ainda **bloqueia o servidor**
por até `REFERENCE_TIMEOUT_MS` (a tentativa de registro roda *depois* de responder
ao client, mas a seguinte espera por ela); (2) um servidor **ocioso** não percebe
que o Reference reiniciou e perdeu o registro: ele só volta à lista no próximo
heartbeat (10 mensagens), porque não há heartbeat por tempo.
(3) no Docker o Reference é um **nome de host** (`reference-service`). O socket
para ele é criado sob demanda, então um nome que ainda não resolve (container do
Reference ausente ou parado) conta como "Reference indisponível" e entra no mesmo
backoff, em vez de derrubar o servidor. No **Java**, a JVM guarda em cache a falha
de DNS por ~10 s (`networkaddress.cache.negative.ttl`), então o registro pode
demorar esse tempo a mais depois que o Reference passa a existir.

Todos os envelopes de aplicação agora contêm `logical_clock`. Cada emissor
incrementa antes de enviar; ao receber, atualiza seu contador para o máximo
entre o local e o recebido, conforme o enunciado. O broker continua apenas
repassando frames Pub/Sub, sem lógica adicional.

O modo `ACTION=BOT_LOOP` implementa o fluxo padronizado: login, completar até
5 canais, assinar até 3 canais e publicar continuamente em grupos de 10
mensagens, escolhendo canal aleatoriamente e aguardando 1 segundo entre
publicações. `ACTION=SUBSCRIBE` também aceita vários canais em `CHANNELS`
separados por vírgula. O log de recebimento exibe canal, mensagem,
timestamp de envio e timestamp local de recebimento.

## Escolhas de projeto

- **Linguagens:** Python e Java. Cada uma tem cliente (bot) e servidor completos,
  e todos falam o mesmo protocolo. O projeto é
  individual, então as duas linguagens atendem ao requisito mínimo.
- **Troca de mensagens:** ZeroMQ. REQ/REP entre bots e servidores (passando pelo
  broker REQ/REP) e entre servidores e Reference Service; PUB/SUB para a
  distribuição das publicações (proxy XSUB/XPUB).
- **Serialização:** MessagePack (binário) em todas as mensagens. Cada envelope
  tem `type`, `sender_id`, `sender_lang`, `timestamp` (ms, físico) e
  `logical_clock`.
- **Persistência:** cada servidor tem o seu diretório de dados, com três
  arquivos MessagePack (`logins`, `channels`, `messages`), cada um contendo uma
  lista de registros. A escrita é atômica (arquivo temporário + rename) e um
  arquivo corrompido é preservado como `*.corrupt` em vez de ser sobrescrito.

## Brokers

O projeto tem **dois** brokers independentes:

- `broker-reqrep/`: `ROUTER` (porta 5555, lado dos bots) e `DEALER` (porta 5556,
  lado dos servidores) ligados por `zmq.proxy`. O `DEALER` distribui as
  requisições dos bots entre os servidores em **round-robin**. Os servidores
  mantêm o `bind` na 5555 e fazem também `connect` no backend
  (`SERVER_BROKER_ADDRESS`), de modo que os bots de demonstração de um passo
  continuam podendo falar direto com um servidor específico.
- `broker/`: proxy Pub/Sub, `XSUB` 5557 / `XPUB` 5558.

**Limitação até a Parte 5:** como cada servidor guarda só uma parte dos dados,
um bot de loop que passa pelo broker REQ/REP pode listar canais em um servidor
e publicar em outro que não conhece o canal. Isso aparece nos logs como
`publicação recusada: canal ... nao existe`. É esperado e será resolvido pela
replicação da Parte 5.

## Logs

Todo envio e recebimento é impresso com o conteúdo completo do envelope:
`[SEND]`/`[RECV]` (cliente-servidor), `[REF-SEND]`/`[REF-RECV]`
(servidor-Reference), `[PUB-SEND]` (publicação no tópico) e `[PUBSUB-RECV]`
(bot recebendo, com `timestamp_envio` e `timestamp_recebimento`).

## Execução Docker

```powershell
docker compose up --build
```

O Compose sobe `reference-service`, `broker` (Pub/Sub), `reqrep-broker`,
`python-server`, `java-server` e os bots. Os bots de demonstração de ação única
terminam com `exit code 0`; `bot-loop-python` e `bot-loop-java` seguem publicando
(fluxo padronizado do enunciado), então o `up` permanece ativo até `Ctrl+C`.
Para rodar em segundo plano use `docker compose up -d` e `docker compose logs -f <serviço>`.

Os dados persistem em `python/server/data/` e `java/server/data/` (bind mount) e
**não** devem ser versionados (estão no `.gitignore`). Como eles sobrevivem ao
`docker compose down`, uma segunda execução sem limpar faz os bots de criação
receberem `canal já existe` e os publishers de demonstração não rodam. Para
recomeçar do zero:

```powershell
docker compose down
Remove-Item -Force .\python\server\data\*.msgpack, .\java\server\data\*.msgpack -ErrorAction SilentlyContinue
docker compose up --build
```

## Validação da Parte 3

Executada em Docker Desktop (Windows), com build limpo e sem dados residuais:

| Verificação | Como | Resultado |
|---|---|---|
| Subida e bots de um passo | `docker compose ps -a` | Todos os bots de login/canal/Pub-Sub terminaram com `Exited (0)`; bots de loop ativos |
| Round-robin e interoperabilidade | `docker compose logs <servidor> \| Select-String "bot-<outra-linguagem>-loop"` | Cada servidor recebeu requisições dos bots de loop das **duas** linguagens |
| Heartbeat | logs do `reference-service` | Cada servidor envia heartbeat a cada ~10 mensagens, com `rank` e `reference_time_ms` (a hora deixou de ser devolvida na Parte 4) |
| Rank estável | `docker compose restart java-server` | O servidor volta com o mesmo rank |
| Expiração e retorno | `python tests\list_servers.py` antes/depois de `stop` + 35 s + `start` | Lista com os 2 servidores → só o Python → os 2 de volta, Java com rank 2 |
| Relógio lógico | logs de bots e servidores | Valores crescem de forma coerente (envio incrementa, recebimento usa o máximo) |
| Persistência | `docker compose down` e `up` sem apagar dados | Servidor carrega logins, canais e mensagens anteriores |
| Testes unitários | `python -m unittest discover -s tests -p "test_part3.py" -v` | 3 testes passam |

`tests/list_servers.py` consulta o Reference (porta 5559 publicada no host);
requer `pip install pyzmq msgpack`.

## Teste de contrato do `PUBLISH_REQUEST` (Python e Java)

`tests/test_publish_validation.py` fala só por sockets ZeroMQ, então o
**mesmo script** testa o servidor Python ou o Java. Cobre string válida,
número, booleano, lista, objeto, `null`, campo ausente e string vazia (mais
variantes "falsy": `0`, `false`, `[]`, `{}`), e confere também que nada
rejeitado foi **publicado** (um subscriber real escuta o canal) nem
**persistido** (compara o número de registros em `messages.msgpack`).

Com o broker e **um** servidor rodando (um de cada vez, ambos usam a 5555):

```powershell
# servidor Python
python tests\test_publish_validation.py --server tcp://localhost:5555 --broker tcp://localhost:5558 --messages-file python\server\data\messages.msgpack

# servidor Java
python tests\test_publish_validation.py --server tcp://localhost:5555 --broker tcp://localhost:5558 --messages-file java\server\data\messages.msgpack
```

Sai com código 1 se algum caso falhar. Dependências: as mesmas do client
Python (`pyzmq`, `msgpack`).

## Demonstração Pub/Sub

O `docker-compose.yml` cria o canal `avisos-clinica` nos dois servidores, um
publisher em cada servidor publica uma mensagem e dois subscribers (Python e Java)
escutam o broker Pub/Sub; cada subscriber deve receber as duas mensagens. Os
publishers esperam 8 s antes de publicar (`sleep` no `command`): o Pub/Sub do
ZeroMQ não sinaliza "assinatura ativa" (*slow joiner*), e sem o atraso a
mensagem poderia sair antes de o subscriber Java terminar de assinar. Em máquinas
lentas, aumente esse valor.

## Limitações conhecidas

- **Dados divergentes entre servidores (até a Parte 5):** o broker REQ/REP
  distribui em round-robin e cada servidor guarda só parte dos dados. Um bot pode
  listar canais em um servidor e publicar em outro que não conhece o canal
  (`publicação recusada: canal ... nao existe`). Esperado; a replicação da
  Parte 5 resolve.
- **Prefix match no Pub/Sub:** assinar `vac` também recebe `vacinas`.
- **Servidores ociosos saem da lista do Reference:** o heartbeat só sai a cada 10
  mensagens de clientes. Sem tráfego por 30 s o servidor some da lista, e um
  servidor que inicia uma eleição nessa janela não o vê. Isso se corrige sozinho:
  o coordenador republica o anúncio a cada 5 s e um anúncio de rank maior sempre
  prevalece. Durante a janela pode haver dois coordenadores por alguns segundos.
- **Detecção de falha do coordenador só na sincronização:** a queda é percebida na
  próxima sincronização (a cada 15 mensagens) ou na próxima eleição, não por
  monitoramento contínuo.
- **Ranks em memória** no Reference.
- **Servidor parado:** uma requisição que o broker entregar a um servidor caído
  expira no bot (`RECV_TIMEOUT_MS`) e o bot de loop pode encerrar. Tratar isso
  entra junto com a tolerância a falhas das próximas partes.

## Validação da Parte 4

A lógica de eleição e sincronização foi exercitada com servidores Python locais
(subida em ordem, queda e volta do coordenador, resposta `OK` de servidor maior).
Para validar em Docker:

```powershell
docker compose down
Remove-Item -Force .\python\server\data\*.msgpack, .\java\server\data\*.msgpack -ErrorAction SilentlyContinue
docker compose up --build -d
Start-Sleep 30
docker compose logs python-server java-server | Select-String "ELEICAO|COORDENADOR|CLOCK-SYNC"
```

| Verificação | Esperado |
|---|---|
| Eleição inicial | `java-server` (rank 2) vira coordenador; `python-server` registra `novo coordenador: java-server` |
| Sincronização | `python-server` mostra `[CLOCK-SYNC] ... correcao=+2000ms` (desfaz o desvio) e depois correções de poucos ms |
| Anúncio | `[PUBSUB-RECV] topic=servers` nos dois servidores, a cada ~5 s |
| Queda do coordenador | `docker compose stop java-server` → no próximo `CLOCK-SYNC` o `python-server` mostra `indisponivel` e `eleito coordenador` |
| Volta do coordenador | `docker compose start java-server` → `java-server` se elege de novo e o `python-server` adota o anúncio |
| Heartbeat sem hora | `docker compose logs reference-service \| Select-String "reference_time_ms"` não retorna nada |

## Próximos passos

Parte 5: replicação dos dados (logins, canais e mensagens) entre todos os
servidores, para que cada um tenha o histórico completo.
