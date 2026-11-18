# vet_flow — Sistema Distribuído de Mensagens (Clínica Veterinária)

> **Status do código:** Partes 1 a 5 implementadas (Python e Java, com
> interoperabilidade entre as linguagens).
> Parte 1: login, criação e listagem de canais, com persistência em disco.
> Parte 2: Pub/Sub via broker (`XSUB` 5557 / `XPUB` 5558), com persistência das
> mensagens publicadas e bots padronizados (`ACTION=BOT_LOOP`).
> Parte 3: relógios lógicos, serviço de referência (rank, lista de servidores e
> heartbeat) e broker REQ/REP com round-robin entre os servidores.
> Parte 4: eleição de coordenador (algoritmo do valentão) e sincronização do
> relógio físico dos servidores com o coordenador.
> Parte 5: replicação ativa, sem líder (Pub/Sub no tópico `replication`), com
> anti-entropia por REQ/REP: todos os servidores guardam todos os dados.
> Todas as partes do enunciado estão implementadas.

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

O contrato de mensagens comum entre Python e Java está resumido na seção
[Formato das mensagens](#formato-das-mensagens).

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
  roteamento), frame 2 = envelope completo em MessagePack. Ver
  [Formato das mensagens](#formato-das-mensagens).
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

## O que já está implementado (Parte 5 — consistência e replicação)

### O problema

O broker REQ/REP distribui as requisições dos bots entre os servidores em
round-robin, então cada servidor recebia só uma parte dos logins, canais e
mensagens. Se um servidor caísse, essa parte do histórico se perdia, e quem pedisse
o histórico a um servidor receberia só a parte dele. Na versão final **todos os
servidores guardam todos os dados**.

### Método escolhido: replicação ativa, sem líder, com consistência eventual

Qualquer servidor aceita escritas (o broker decide qual) e as propaga a todos os
outros. Não há servidor primário: os servidores são pares. A consistência é
**eventual**: todos convergem para o mesmo conjunto de dados, na mesma ordem, mas
um servidor pode estar alguns instantes atrás de outro. Para cobrir a perda de
mensagens do Pub/Sub, há **anti-entropia** (cada servidor compara o que tem com um
par e puxa o que falta).

Por que não primário-backup usando o coordenador da Parte 4: ele serializaria todas
as escritas (consistência mais forte), mas os servidores deixariam de aceitar
escritas sem coordenador, e cada escrita passaria por um salto extra. Para este
projeto (bots que só acrescentam dados) a consistência eventual basta e é mais
simples de manter disponível. O coordenador continua só como referência de relógio.

### Como foi implementado

1. **Identificação:** todo registro (login, canal ou mensagem) recebe um id
   `(origin, seq)`: `origin` é o servidor que o gravou e `seq` um contador por
   origem. Também leva `lc`, o relógio lógico no momento da gravação.
2. **Escrita:** o servidor grava em disco **e só então** publica o registro no
   tópico `replication` (`REPLICATION`), depois responde ao bot. Os demais servidores
   assinam esse tópico no `XPUB`, aplicam o registro e o gravam no próprio disco.
   O registro replicado **não** é republicado no canal dos bots (isso duplicaria a
   mensagem).
3. **Idempotência:** aplicar duas vezes o mesmo id não tem efeito, então duplicatas
   (eco da própria publicação, recebimento por Pub/Sub e por anti-entropia) são
   inofensivas.
4. **Anti-entropia (`SYNC_REQUEST`/`SYNC_RESPONSE`):** cada servidor mantém, por
   origem, o maior `seq` sem buracos. Ele pede a um par os registros acima disso, em
   lotes de até 500. Isso acontece (a) ao iniciar, com todos os pares conhecidos;
   (b) quando um registro chega fora de ordem (buraco); (c) periodicamente, com um
   par por vez, a cada ~10 s (variação aleatória de ±30%). Um servidor com o disco
   vazio se reconstrói por esse mecanismo, inclusive os registros que ele mesmo
   tinha originado.
5. **Ordem:** os registros são ordenados por `(lc, origin, seq)`, o mesmo critério
   em todos os servidores; a lista de canais e o histórico saem iguais em qualquer
   um.
6. **Criações concorrentes do mesmo canal:** duas criações simultâneas do mesmo nome
   em servidores diferentes são aceitas localmente; depois de replicadas, vale a de
   menor `(lc, origin, seq)` em todos (a outra fica no disco, mas o nome aparece
   uma vez). Depois da replicação, criar um nome que já existe em qualquer servidor
   é recusado.
7. **Leitura do histórico:** `HISTORY_REQUEST` (`{channel_name, limit}`, até 500)
   devolve as últimas mensagens do canal, em ordem; qualquer servidor responde.
8. **Tópicos reservados:** `servers` (Parte 4) e `replication` não podem ser nomes
   de canal.

### Mudanças e adaptações em relação ao método

- **Transporte:** em vez de um multicast confiável, usa-se o Pub/Sub que já existe
  (sem garantia de entrega). A anti-entropia é o que compensa; é uma adaptação ao
  que o projeto oferece.
- **Id por origem e `lc`:** necessários para detectar buracos, ignorar duplicatas
  e ordenar igual em todos os servidores.
- **Espera que também atende:** os servidores se pedem dados uns aos outros. Dois
  servidores que se pedem algo ao mesmo tempo ficariam bloqueados até o timeout (o
  que também derrubava a eleição). Por isso, enquanto aguarda a resposta de um par
  ou do Reference, o servidor continua atendendo o `REP` (só responde; heartbeat e
  demais tarefas voltam para o loop principal). Continua sem threads.
- **Heartbeat e sincronização de relógio** passaram a usar a contagem acumulada de
  mensagens de clientes (mensagens atendidas durante uma espera também contam).
- **Persistência:** os servidores mantêm as listas em memória e regravam o arquivo
  (escrita atômica); registros replicados recebidos no mesmo ciclo são gravados de
  uma vez. Dados de versões anteriores (sem id) são migrados ao iniciar.
- Bots, brokers e Reference **não** mudaram.

### Garantias e limitações

- **Convergência:** enquanto os servidores se enxergam, os dados convergem em
  segundos mesmo com perda de mensagens do Pub/Sub ou queda e volta de servidores.
- **Servidor que cai com um registro ainda não replicado** (gravou em disco, mas
  caiu antes de publicar): o registro aparece nos demais quando ele voltar. Se o
  disco dele for perdido junto, o registro se perde.
- **Buraco permanente:** se um registro nunca chega, os `seq` seguintes dessa origem
  continuam sendo reenviados em cada anti-entropia (limitado a um lote por rodada).
- **Disco apagado de um servidor com os pares no ar:** ele se reconstrói, mas
  gravações de clientes recebidas **antes** da primeira anti-entropia poderiam
  reutilizar ids. Na prática a sincronização inicial roda assim que o servidor
  se registra no Reference, antes dos bots.
- **Custo:** cada escrita regrava o arquivo inteiro da lista; adequado a este
  porte, não a milhões de registros. Não há compactação nem remoção de dados.
- **Relógios lógicos:** a ordem do histórico respeita causalidade, mas mensagens
  concorrentes de bots diferentes não têm ordem "real" definida; o desempate é
  `(origin, seq)`.

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
│   ├── list_servers.py              # consulta a lista de servidores no Reference
│   └── check_replication.py         # compara canais e históricos entre os servidores (Parte 5)
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
| `REPL_SYNC_SECONDS` | server | `10` | Intervalo (±30%) da anti-entropia periódica com um par |
| `REPL_BATCH` | server | `500` | Máximo de registros por `SYNC_RESPONSE` |
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
  e todos falam o mesmo protocolo (ver [Formato das mensagens](#formato-das-mensagens)). O projeto é
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

## Formato das mensagens

Todas as mensagens (REQ/REP e Pub/Sub) são serializadas em **MessagePack** e
usam o mesmo envelope, em qualquer linguagem:

| Campo | Descrição |
|---|---|
| `type` | Tipo da mensagem (tabela abaixo) |
| `sender_id` | Nome de quem envia (bot ou servidor) |
| `sender_lang` | `python` ou `java` |
| `timestamp` | Hora física do envio, em ms (Unix epoch) |
| `logical_clock` | Relógio lógico de quem envia |
| `payload` | Mapa com os dados do tipo |

Nas respostas, `payload.status` é `OK` ou `ERROR` (com `error_msg`).

| Tipo | Canal | Payload |
|---|---|---|
| `LOGIN_REQUEST` / `LOGIN_RESPONSE` | REQ/REP bot→servidor | `{bot_name}` / `{status}` |
| `CHANNEL_CREATE_REQUEST` / `CHANNEL_CREATE_RESPONSE` | REQ/REP | `{channel_name, bot_name}` / `{status}` |
| `CHANNEL_LIST_REQUEST` / `CHANNEL_LIST_RESPONSE` | REQ/REP | `{}` / `{channels: [nomes]}` |
| `PUBLISH_REQUEST` / `PUBLISH_RESPONSE` | REQ/REP | `{channel_name, message}` / `{status}` |
| `CHANNEL_MESSAGE` | PUB/SUB, tópico = nome do canal (2 frames: tópico + envelope) | `{channel_name, message}` |
| `REFERENCE_REGISTER_REQUEST` | REQ/REP servidor→Reference | `{server_name}` → `{status, rank}` |
| `REFERENCE_LIST_REQUEST` | REQ/REP | `{}` → `{status, servers: [{name, rank}]}` |
| `REFERENCE_HEARTBEAT_REQUEST` | REQ/REP | `{server_name}` → `{status, rank}` |
| `ELECTION_REQUEST` / `ELECTION_RESPONSE` | REQ/REP servidor→servidor | `{rank}` / `{status, rank}` |
| `CLOCK_REQUEST` / `CLOCK_RESPONSE` | REQ/REP servidor→coordenador | `{}` / `{status, time_ms}` |
| `COORDINATOR_ANNOUNCEMENT` | PUB, tópico `servers` | `{coordinator, rank}` |
| `HISTORY_REQUEST` / `HISTORY_RESPONSE` | REQ/REP bot→servidor | `{channel_name, limit?}` / `{status, channel_name, total, messages: [{origin, seq, sender_id, sender_lang, message, timestamp}]}` |
| `REPLICATION` | PUB, tópico `replication` | `{kind: login\|channel\|message, record}` (o registro leva `origin`, `seq`, `lc`) |
| `SYNC_REQUEST` / `SYNC_RESPONSE` | REQ/REP servidor→servidor | `{have: {origem: maior seq sem buracos}, limit}` / `{status, records: [{kind, record}], more}` |
| `ERROR_RESPONSE` | REP | `{status: "ERROR", error_msg}` (mensagem inválida ou tipo desconhecido) |

## Brokers

O projeto tem **dois** brokers independentes:

- `broker-reqrep/`: `ROUTER` (porta 5555, lado dos bots) e `DEALER` (porta 5556,
  lado dos servidores) ligados por `zmq.proxy`. O `DEALER` distribui as
  requisições dos bots entre os servidores em **round-robin**. Os servidores
  mantêm o `bind` na 5555 e fazem também `connect` no backend
  (`SERVER_BROKER_ADDRESS`), de modo que os bots de demonstração de um passo
  continuam podendo falar direto com um servidor específico.
- `broker/`: proxy Pub/Sub, `XSUB` 5557 / `XPUB` 5558.

**Dados entre servidores:** o `DEALER` entrega cada requisição a um servidor diferente,
então um bot pode listar canais em um servidor e publicar em outro. Desde a Parte 5
todos os servidores guardam todos os dados (ver "Parte 5"), de modo que qualquer um
atende qualquer requisição.

## Logs

Todo envio e recebimento é impresso com o conteúdo completo do envelope:
`[SEND]`/`[RECV]` (cliente-servidor), `[REF-SEND]`/`[REF-RECV]`
(servidor-Reference), `[PUB-SEND]` (publicação no tópico) e `[PUBSUB-RECV]`
(bot recebendo, com `timestamp_envio` e `timestamp_recebimento`). Na replicação:
`[REPLICA-SEND]`/`[REPLICA-RECV]` (um registro por mensagem, conteúdo completo) e,
para os lotes de anti-entropia (até 500 registros), um resumo em
`[PEER-SEND]`/`[PEER-RECV]` e `[SYNC]`.

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

O `docker-compose.yml` cria o canal `avisos-clinica` **uma vez**, no servidor Python
(desde a Parte 5 os canais são replicados, então criar o mesmo nome nos dois
servidores seria recusado com `canal já existe`). Um publisher em cada servidor publica
uma mensagem, o do servidor Java em um canal criado no outro servidor, e dois
subscribers (Python e Java) escutam o broker Pub/Sub; cada subscriber deve receber as
duas mensagens. Os
publishers esperam 8 s antes de publicar (`sleep` no `command`): o Pub/Sub do
ZeroMQ não sinaliza "assinatura ativa" (*slow joiner*), e sem o atraso a
mensagem poderia sair antes de o subscriber Java terminar de assinar. Em máquinas
lentas, aumente esse valor.

## Limitações conhecidas

- **Consistência eventual (Parte 5):** logo depois de uma escrita, um servidor pode
  ainda não ter o registro (janela de milissegundos no caso normal; até a próxima
  anti-entropia se uma publicação se perder). Ver "Garantias e limitações" na Parte 5.
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

Executada em Docker Desktop (Windows), com build limpo e sem dados residuais
(`docker compose down`, apagar os `.msgpack`, `docker compose up --build -d`):

| Verificação | Resultado |
|---|---|
| Eleição inicial | Os dois se elegem no início (ainda não se enxergam no Reference); em seguida o `python-server` recebe o anúncio de rank 2 e passa a tratar o `java-server` como coordenador |
| Sincronização (Python → Java) | `python-server` corrige +2014 ms (desfaz o `CLOCK_SKEW_MS=-2000`) e depois −9 ms, −2 ms; o ajuste converge para ~2003 ms. `java-server` fica com `ajuste=0ms` (referência) |
| Anúncio periódico | `COORDINATOR_ANNOUNCEMENT` do `java-server` no tópico `servers` a cada ~5 s |
| Heartbeat sem hora | `reference_time_ms` não aparece mais nos logs do `reference-service` |
| Queda do coordenador | `docker compose stop java-server`: no próximo `CLOCK-SYNC` o `python-server` mostra `indisponivel`, inicia a eleição, o `java-server` não responde e o `python-server` é eleito coordenador |
| Volta do coordenador | `docker compose start java-server`: o `python-server` adota o anúncio do `java-server` (`novo coordenador: java-server`) e volta a sincronizar, com correções de poucos ms |

A lógica também foi exercitada com servidores Python locais (ordem de subida,
queda e volta do coordenador, resposta `OK` de servidor de rank maior).

Para repetir:

```powershell
docker compose down
Remove-Item -Force .\python\server\data\*.msgpack, .\java\server\data\*.msgpack -ErrorAction SilentlyContinue
docker compose up --build -d
Start-Sleep 30
docker compose logs python-server java-server | Select-String "ELEICAO|COORDENADOR|CLOCK-SYNC"
docker compose stop java-server; Start-Sleep 25
docker compose logs python-server | Select-String "indisponivel|ELEICAO|COORDENADOR" | Select-Object -Last 6
docker compose start java-server; Start-Sleep 25
docker compose logs python-server | Select-String "COORDENADOR|CLOCK-SYNC" | Select-Object -Last 6
```

## Validação da Parte 5

**Em Docker:**

```powershell
docker compose down
Remove-Item -Force .\python\server\data\*.msgpack, .\java\server\data\*.msgpack -ErrorAction SilentlyContinue
docker compose up --build -d
Start-Sleep 45
python tests\check_replication.py        # compara canais e históricos dos dois servidores
docker compose logs python-server java-server | Select-String "REPLICA-RECV" | Select-Object -Last 4
docker compose logs bot-loop-python bot-loop-java | Select-String "nao existe"   # (quase) nenhum
```

`tests/check_replication.py` consulta o servidor Python (`localhost:5555`) e o Java
(`localhost:5556`), compara a lista de canais e o histórico de cada canal (ignora as
mensagens dos últimos 10 s, porque os bots continuam publicando) e repete por até
30 s. Imprime `IGUAIS` e termina com código 0 quando os dois coincidem
(requer `pip install pyzmq msgpack`).

Recuperação depois de uma queda:

```powershell
docker compose stop java-server; Start-Sleep 20
docker compose start java-server; Start-Sleep 25
docker compose logs java-server | Select-String "sincronizado com"
python tests\check_replication.py
```

O esperado é `[SYNC] sincronizado com python-server: N registro(s) novo(s)` no Java
(as mensagens que ele perdeu) e `IGUAIS` no final.

**Em teste local** (dois servidores Python, broker e Reference reais): escritas em
ambos os servidores aparecem nos dois com a mesma ordem; criar um canal que existe no
outro servidor é recusado; um servidor parado perde 5 mensagens e as recupera ao
voltar; um servidor com o disco apagado se reconstrói por completo (inclusive os
próprios registros) e as escritas seguintes dele continuam a numeração; dois
servidores que se pedem dados ao mesmo tempo (inicialização simultânea) não se
bloqueiam.

## Estado do projeto

As cinco partes do enunciado estão implementadas: login e canais (1), Pub/Sub e
persistência (2), relógios lógicos e referência (3), eleição e sincronização de
relógio (4) e replicação dos dados (5).
