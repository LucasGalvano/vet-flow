# vet_flow — Sistema Distribuído de Mensagens (Clínica Veterinária)

> **Status atual do projeto: Parte 1 completa e dockerizada.**
> Login, criação e listagem de canais, com persistência em disco, em
> **Python e Java**. Interoperabilidade entre as duas linguagens
> confirmada nos dois sentidos, para os três tipos de mensagem — inclusive
> via `docker compose up`, sem interação manual. Este README documenta
> apenas o que já existe — não descreve funcionalidades futuras como já
> implementadas.

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

O contrato de mensagens comum entre Python e Java está documentado em
[`protocol/PROTOCOL.md`](protocol/PROTOCOL.md) e é atualizado conforme cada
parte é implementada.

## O que já está implementado (Parte 1 — completa)

- `python/server` e `java/server`: servidor ZeroMQ (`REP`) que trata
  `LOGIN_REQUEST`, `CHANNEL_CREATE_REQUEST` e `CHANNEL_LIST_REQUEST`, com
  persistência em disco (MessagePack) para logins e canais.
- `python/client` e `java/client`: bot ZeroMQ (`REQ`) que envia uma dessas
  requisições por execução, escolhida via variável `ACTION`.
- Nome de canal duplicado é rejeitado com erro (ambas as linguagens).
- **Interoperabilidade Python ↔ Java confirmada** nos dois sentidos, para
  os três tipos de mensagem — incluindo o caso mais importante: um canal
  criado por um bot Java, persistido por um servidor Python, lido de
  volta corretamente por um client Python.

## Estrutura de diretórios

```
projeto/
├── .gitignore
├── .env.example
├── README.md
├── protocol/
│   └── PROTOCOL.md        # contrato comum de mensagens Python <-> Java
├── python/
│   ├── server/
│   │   ├── server.py
│   │   ├── persistence.py
│   │   └── requirements.txt
│   └── client/
│       ├── client.py
│       └── requirements.txt
└── java/
    ├── server/
    │   ├── pom.xml
    │   └── src/main/java/com/vetflow/server/
    │       ├── Server.java
    │       └── Persistence.java
    └── client/
        ├── pom.xml
        └── src/main/java/com/vetflow/client/
            └── Client.java
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
| `ACTION` | client | `LOGIN` | Ação a executar: `LOGIN`, `CHANNEL_CREATE` ou `CHANNEL_LIST` |
| `CHANNEL_NAME` | client | `avisos-gerais` | Nome do canal, usado quando `ACTION=CHANNEL_CREATE` |
| `SERVER_BIND_ADDRESS` | server | `tcp://*:5555` | Endereço de bind do servidor |
| `LOGINS_PERSISTENCE_PATH` | server | `python/server/data/logins.msgpack` | Caminho do arquivo de persistência de logins |
| `CHANNELS_PERSISTENCE_PATH` | server | `python/server/data/channels.msgpack` | Caminho do arquivo de persistência de canais |

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

## Docker

```bash
docker compose up --build
```

**Validado de ponta a ponta** (build limpo, sem dados residuais): sobe
`python-server` e `java-server` (ficam de pé) e roda 6 bots
automaticamente, sem nenhuma interação manual — login nas 4 combinações
de linguagem, mais um canal criado por um bot Java no servidor Python e
listado de volta por um bot Python. Todos os containers de bot terminam
com `exit code 0`.

Dados persistem em `python/server/data/` e `java/server/data/` no host
(bind mount), sobrevivendo a `docker compose down`.

**Nota:** como os dados sobrevivem no host, rodar a demonstração de novo
sem limpar os dados vai fazer o `bot-channel-create` falhar com
`canal já existe` (comportamento correto — é a regra de duplicidade
funcionando). Para rodar do zero:

```powershell
docker compose down
Remove-Item -Force .\python\server\data\*.msgpack -ErrorAction SilentlyContinue
Remove-Item -Force .\java\server\data\*.msgpack -ErrorAction SilentlyContinue
docker compose up --build
```

## Próximos passos

1. Parte 2 (Pub/Sub + broker).