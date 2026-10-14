# CONTEXTO.md — leia isto antes de mexer no projeto

Este arquivo existe para quem entra no projeto (pessoa **ou LLM**) entender
**o que é, por que foi feito assim e em que pé está**, sem ter participado
da conversa em que foi construído. 

Para manter a clareza e evitar perda de contexto da IA, **não cole múltiplos arquivos dentro deste arquivo**.
Forneça os arquivos de forma modular (anexando-os separadamente na conversa da LLM ou referenciando-os no editor de código).

Este documento complementa, não substitui:

- [`README.md`](../README.md) — como executar e testar (atualizado a cada etapa);
- [`protocol/PROTOCOL.md`](../protocol/PROTOCOL.md) — contrato de mensagens Python ↔ Java
  (**fonte de verdade do protocolo**).

> **Como manter este arquivo.** Ele tem três tipos de seção:
> **ESTÁVEL** (quase nunca muda), **CRESCE** (só se acrescenta; nunca reescreva
> uma decisão antiga, marque-a como substituída) e **VOLÁTIL** (atualizar a cada
> parte concluída). Última atualização: implementação da Parte 3 na branch `entrega-parte-3`.

---

## 0. Legenda de evidência (importante)

Em todo status deste arquivo, "funciona" sem etiqueta **não vale**. Use:

| Etiqueta | Significa |
|---|---|
| **[EXEC-DOCKER]** | executado com `docker compose up --build` na máquina de um dos integrantes |
| **[EXEC-LOCAL]** | executado pelos integrantes, fora do Docker, na máquina deles |
| **[EXEC-SANDBOX]** | executado pelo assistente em ambiente isolado (Linux), **não** na máquina de vocês |
| **[LIDO]** | só inspecionado no código; **nunca executado** |

Regra do projeto: **nunca afirmar que algo foi testado se só foi lido.**

---

## 1. Objetivo e enunciado  *(ESTÁVEL)*

Projeto acadêmico de **Sistemas Distribuídos**, feito em **dupla**: um sistema
distribuído de troca de mensagens com **ZeroMQ**, containers e Docker Compose,
com domínio de **clínica veterinária** (registro de validade de produtos,
limpeza da clínica, avisos, histórico de operações). Cada integrante é dono de
uma linguagem (**Python** e **Java**), mas **as duas implementam o sistema
completo** (client + server) como réplicas heterogêneas do **mesmo protocolo**.

### 1.1 Texto original do enunciado

> ⚠️ **PENDENTE:** colar aqui o texto original da disciplina. O resumo abaixo foi
> **relatado de memória por um integrante**, não copiado do enunciado. Quando
> houver conflito entre este arquivo e o enunciado, **vale o enunciado**.

```
<COLE AQUI O ENUNCIADO ORIGINAL>
```

### 1.2 Resumo dos requisitos (como relatados — conferir com o original)

**5 partes obrigatórias:**

1. Login de usuários/bots, criação e listagem de canais, e persistência.
2. Publicação/assinatura (Pub/Sub) em canais.
3. Relógios lógicos, heartbeat e sincronização de servidores.
4. Eleição de coordenador e sincronização de relógios (algoritmo de Berkeley).
5. Consistência e replicação dos dados entre os servidores.

**Requisitos transversais:**

- Comunicação com **ZeroMQ**; execução com **Docker/Podman + Docker Compose**,
  pela **rede criada pelo Compose**; tudo sobe com **`docker compose up`**.
- **Sem interação manual** na entrega; os usuários são **bots**.
- Clients e servers rodando **simultaneamente**, múltiplos de cada.
- Toda mensagem tem **timestamp**.
- Mensagens serializadas em **formato binário conhecido** (MessagePack,
  Protobuf…). **JSON, XML e texto puro são proibidos** como formato transmitido.
- **Persistência em disco.**
- Pelo menos **2 linguagens** (se individual); cada uma implementa as partes relevantes.

**Detalhes por parte (relatados pelo integrante a partir do enunciado):**

| Parte | Detalhe |
|---|---|
| 1 | `Client --REQ--> Server`, `Client <--REP-- Server` (login, criar/listar canais) |
| 2 | Proxy Pub/Sub **separado**, `XSUB` na porta **5557**, `XPUB` na porta **5558** |
| 3 | Um **"reference process"**: recebe o registro dos servidores, atribui/gerencia **ranks**, mantém a **lista de servidores disponíveis**, é consultado pelos servidores |
| 4 | **Coordenador eleito entre os próprios servidores**; fornece a hora correta e executa o **Berkeley** |

---

## 2. Arquitetura  *(ESTÁVEL; cresce a cada parte)*

```
                     Reference Service            (Parte 3 — implementado nesta branch)
                     registro / ranks / lista
                              │
          ┌───────────────────┴───────────────────┐
     Python Server ◄──────────────────────► Java Server
          │  (comunicação direta entre servidores: Partes 3-5, a definir)
          │
          │ PUB                                   PUB │
          └──────────────► [ XSUB  Broker  XPUB ] ◄───┘
                            5557          5558
                                            │ SUB
                              Python Client / Java Client
```

**Distinções que NÃO podem ser misturadas:**

- **Reference Service** (Parte 3): processo **separado e fixo**; registro, ranks, lista de servidores.
- **Coordenador** (Parte 4): **um dos servidores**, **eleito dinamicamente**; Berkeley.
  **Não** transformar o Reference Service em coordenador.
- **Broker**: processo separado, **só Pub/Sub**, sem lógica de negócio. Heartbeat,
  eleição e replicação **não** passam por ele (comunicação direta entre servidores).

### 2.1 Estrutura do repositório

```
.
├── README.md  .env.example  .gitignore  docker-compose.yml
├── docs/CONTEXTO.md
├── reference/  reference.py  registry.py  Dockerfile  requirements.txt
├── protocol/PROTOCOL.md              # contrato de mensagens (fonte de verdade)
├── tests/test_publish_validation.py  # contrato do PUBLISH_REQUEST (serve p/ Python E Java)
├── broker/                           # proxy XSUB/XPUB (Python) + Dockerfile
│   ├── broker.py  requirements.txt  Dockerfile
│   └── test_pub.py  test_sub.py      # scripts de teste manual do broker
├── python/
│   ├── server/  server.py  persistence.py  requirements.txt  Dockerfile
│   └── client/  client.py  requirements.txt  Dockerfile
└── java/                             # Maven; estrutura de pastas é OBRIGATÓRIA (ver §7)
    ├── server/  pom.xml  Dockerfile
    │   └── src/main/java/com/vetflow/server/  Server.java  Persistence.java
    └── client/  pom.xml  Dockerfile
        └── src/main/java/com/vetflow/client/  Client.java
```

### 2.2 Portas

| Serviço | Porta | Socket |
|---|---|---|
| Python Server | 5555 | `REP` (bind) — host `5555` no Compose |
| Java Server | 5555 *(no container)* | `REP` (bind) — host **5556** no Compose |
| Broker | 5557 / 5558 | `XSUB` / `XPUB` (bind) |
| Reference Service | 5559 | `REP` (bind) |

Dentro do Compose, servidores e bots se enxergam pelo **nome do serviço**
(`tcp://python-server:5555`, `tcp://broker:5557`…), **nunca `localhost`**.
Fora do Docker, os dois servidores usam a 5555 por padrão: rode **um por vez**
ou suba o Java com `SERVER_BIND_ADDRESS=tcp://*:5556`.

### 2.3 Formato das mensagens (resumo — o detalhe está no `PROTOCOL.md`)

Envelope (mapa MessagePack), igual em REQ/REP e em Pub/Sub:

```
{ type: str, sender_id: str, sender_lang: "python"|"java", timestamp: int (epoch ms), logical_clock: int, payload: map }
```

- `sender_lang` é **só depuração** (ver nos logs que a interoperabilidade acontece); nenhuma lógica depende dele.
- `logical_clock` é obrigatório desde a Parte 3. Antes de enviar, o emissor incrementa; ao receber, atualiza para `max(local, recebido)`.
- Todo `*_RESPONSE` tem `status: "OK"|"ERROR"`; em erro, `error_msg`.
- Tipos hoje: `LOGIN_*`, `CHANNEL_CREATE_*`, `CHANNEL_LIST_*`, `PUBLISH_*`, `CHANNEL_MESSAGE` (Pub/Sub), `ERROR_RESPONSE` (genérico).
- **Pub/Sub é multipart:** frame 1 = tópico (nome do canal, UTF-8 puro — é **roteamento**,
  não "a mensagem"); frame 2 = envelope completo em MessagePack.

### 2.4 Persistência

- Arquivos MessagePack, **um por tipo de dado e por servidor**: `logins.msgpack`,
  `channels.msgpack`, `messages.msgpack` (em `<server>/data/`, ignorado pelo git;
  montado como *bind mount* no Compose).
- Formato: **uma lista**; a cada escrita lê-se tudo, acrescenta-se e **regrava-se o arquivo inteiro**.
- Python e Java têm **arquivos independentes** (de propósito: a Parte 5 precisa de réplicas que convergem).
- Premissa de concorrência: **um único processo, uma requisição `REP` por vez**,
  sem locks. O heartbeat ocorre sincronamente no loop do servidor a cada 10
  mensagens; não há thread de acesso concorrente à persistência.

---

## 3. Regras de trabalho  *(ESTÁVEL)*

1. **O enunciado é a fonte de verdade.** Se uma ideia (inclusive de LLM) conflita com ele, **avise** em vez de seguir a ideia.
2. **Não troque decisões já registradas (§4) sem avisar e registrar a mudança.** Não substitua um mecanismo que o enunciado especifica por outro "tecnicamente melhor".
3. **Passos pequenos**, nesta ordem: requisitos → componentes → decisão (com alternativas e trade-offs) → implementação mínima → como testar → verificar erros → só então avançar.
4. **Não invente requisitos** nem adicione tecnologia sem necessidade. Se o enunciado for ambíguo, **pergunte**.
5. **Python e Java implementam o mesmo contrato, de forma independente.** Um não é "referência" que o outro copia; a interoperabilidade é o teste.
6. **Marque** o que é `[REQUISITO DO PROJETO]`, `[DECISÃO DE IMPLEMENTAÇÃO]` e `[SUGESTÃO OPCIONAL]`. Sugestão não é requisito.
7. **Teste antes de afirmar** (ver §0). Prefira testes que **falham no código antigo** — assim se prova que o teste detecta algo.
8. **Mudou o protocolo?** Atualize `PROTOCOL.md` **nas duas linguagens** e este arquivo.
9. **Commits pequenos**, um por passo, no estilo `feat(...)`, `fix(...)`, `docs(...)`, `test(...)`, em português.
10. Sem refatoração não relacionada ao passo em curso.

---

## 4. Registro de decisões  *(CRESCE — só acrescente)*

| # | Decisão | Por quê / alternativas descartadas |
|---|---|---|
| D1 | **`REQ/REP`** entre client e server na Parte 1 | **Exigido pelo enunciado.** Foi sugerido `ROUTER/DEALER` (concorrência) e **rejeitado**: não trocar mecanismo especificado. Consequência: o server atende **uma requisição por vez** |
| D2 | Reference Service e Coordenador são **coisas distintas** | Enunciado: Parte 3 pede um processo de referência; Parte 4 pede eleição entre servidores |
| D3 | **Broker separado**, só Pub/Sub; comunicação servidor↔servidor **direta** | Enunciado exige proxy separado (5557/5558). Broker único para tudo viraria ponto único de falha e atrapalha a replicação |
| D4 | **MessagePack** na rede **e** no disco | Um único formato; atende "binário conhecido"; evita SQLite (tecnologia extra) |
| D5 | Persistência = **lista MessagePack regravada inteira** | Simples. **Alternativa adiada:** log *append-only* — **reavaliar na Parte 5** (replicação costuma pedir log de operações) |
| D6 | Persistência **independente por servidor** | Réplicas que precisam convergir (Parte 5) |
| D7 | Canal **duplicado = `ERROR`**, comparação exata *case-sensitive* | Enunciado não define; é o comportamento comum |
| D8 | **Stack:** Python = `pyzmq` + `msgpack`; Java = **Maven**, **JeroMQ** (Java puro), `jackson-dataformat-msgpack`; Java 17+ | JeroMQ evita `libzmq` nativa no container; Jackson deixa o envelope como `Map`, espelhando o `dict` do Python. Confirmado: JeroMQ ↔ pyzmq interoperam |
| D9 | `sender_lang` no envelope, **só depuração** | Não é requisito |
| D10 | **`logical_clock` fora do envelope até a Parte 3** | Não carregar campo sem uso |
| D11 | **`ERROR_RESPONSE`** genérico + `try/except` **por mensagem** no loop do server | O server **nunca** pode cair por entrada ruim. REP exige **exatamente 1 `send` por `recv`**, então mesmo em erro se responde. (Antes: bytes malformados derrubavam o processo) |
| D12 | Config por **variáveis de ambiente** (`os.environ`/`System.getenv`), com padrões seguros; **sem `python-dotenv`** | Menos dependência; `.env.example` só documenta |
| D13 | **Persistir ANTES de publicar** | Durabilidade antes de visibilidade. Se publicar falhar depois de persistir, o server **loga e responde `OK`** (retry do client duplicaria a mensagem). `OK` = "persistido", **não** "entregue" |
| D14 | `PUBLISH` exige **canal existente** | Canais da Parte 1 são a fonte de verdade |
| D15 | Mensagem do `PUBLISH` é **texto livre**, sem schema de domínio | Parte 2 avalia o mecanismo Pub/Sub; schema por evento fica para depois |
| D16 | **Tópico = nome do canal cru** (sem delimitador) | Simples. **Limitação aceita:** *prefix match* do ZeroMQ (assinar `vac` recebe `vacinas`) |
| D17 | Broker **só em Python** (`zmq.proxy`) | Sem lógica de negócio; duplicar em Java não agrega |
| D18 | No Compose, ordem por `depends_on: service_completed_successfully`; publishers com **`sleep 8`** | O `sleep` é exceção justificada: Pub/Sub não tem sinal de "assinatura ativa" (*slow joiner*) |
| D19 | **`PUBLISH_REQUEST`: `channel_name` e `message` são strings não vazias, sem conversão implícita**, mesmas mensagens de erro em Python e Java | Antes o Python guardava número cru e o Java convertia para texto, e divergiam. Contrato em `PROTOCOL.md` |
| D20 | Reference Service em Python, porta 5559, MessagePack/REQ-REP | Serviço separado; ranks estáveis por nome e disponibilidade em memória |
| D21 | Timeout padrão de disponibilidade de 30s, configurável | O enunciado não fixa quanto tempo esperar para detectar ausência |
| D22 | Heartbeat executado no loop sequencial do REP | Cada décima mensagem pode pausar o servidor durante a chamada ao serviço de referência |
| D23 | `ACTION=BOT_LOOP` implementa o fluxo contínuo exigido | Completa até cinco canais, assina até três e publica 10 mensagens por ciclo, com 1s entre mensagens |

---

## 5. Status por parte, com evidência  *(VOLÁTIL — atualizar a cada etapa)*

### Parte 1 — login, canais, persistência: **concluída**

- Python e Java implementados (`LOGIN`, `CHANNEL_CREATE`, `CHANNEL_LIST`, persistência). 
- Interoperabilidade nas 4 combinações client×server: **[EXEC-LOCAL]**.
- `docker compose up --build` com os bots da Parte 1 (login ×4, canal criado por Java no server Python e listado por Python): **[EXEC-DOCKER]**. Persistência sobrevive a `docker compose down` + `up --build`: **[EXEC-DOCKER]**.
- Robustez (bytes malformados, tipos errados, tipo desconhecido, servidor indisponível, restart): Python **[EXEC-SANDBOX]**; o `try/catch` por mensagem do Java foi exercitado com bytes inválidos e `payload` lista/string/`null` contra o servidor Java: **[EXEC-LOCAL]**.

### Parte 2 — Pub/Sub: **concluída**

- Broker + `PUBLISH`/`SUBSCRIBE` em Python e Java.
- Cenário cruzado (servidor Python→subscriber Java, servidor Java→subscriber Python, Java↔Java, Python↔Python): **[EXEC-LOCAL]**.
- Compose da Parte 2 (canal criado nos dois servidores, um publisher em cada, 2 subscribers; cada subscriber recebeu **as 2** mensagens): **[EXEC-DOCKER]**.
- Robustez do `PUBLISH`: Python **[EXEC-SANDBOX]** (campos inválidos, canal inexistente, restart, broker fora do ar, subscriber sem publicações, 1 MB e 10 MB, canal com acento, falha após persistir). Java: campos inválidos **[EXEC-LOCAL]**; **broker fora do ar, mensagem grande, acento e falha após persistir: [LIDO]**.

- `ACTION=BOT_LOOP` nos clients Python e Java, assinatura múltipla e exibição dos timestamps de envio/recebimento: **[LIDO]** nesta branch.

### Validação de `PUBLISH_REQUEST` (D19)

- Python e Java: teste `tests/test_publish_validation.py` — **19/19 PASS** e **falha no código antigo** dos dois: **[EXEC-SANDBOX]**. O Java do sandbox foi montado com dependências compiladas à mão (Jackson 2.15.2, não o 2.16.1 do `pom.xml`), **não** com `mvn package`.
- Na máquina de vocês: as 17 verificações de resposta passaram **[EXEC-LOCAL]**, mas o check de publicação falhou com `recebido: []` (Pub/Sub inoperante — provavelmente broker fora do ar) e a persistência **não foi verificada** (faltou `--messages-file`).
- **Ainda não confirmado:** rodar o teste completo (broker no ar + `--messages-file`) **no Python e no Java com `mvn package`**, e reexecutar `docker compose up --build` depois da mudança.

### Parte 3 — relógios, heartbeat e Reference Service: **implementada nesta branch; validação ponta a ponta pendente**

- Reference Service, ranks estáveis, listagem e expiração: **[LIDO]**; 3 testes unitários do registro passaram **[EXEC-SANDBOX]**.
- Relógio lógico em clients/servers Python/Java, offset físico e heartbeat por 10 mensagens: **[LIDO]**.
- Compose atualizado e YAML validado com PyYAML: **[EXEC-SANDBOX]**.
- Docker Compose e build Java/Maven ainda não executados: Docker, Maven, `javac` e `pyzmq` não estão disponíveis neste ambiente.

### Partes 4 e 5: **não iniciadas**

---

## 6. Como rodar e testar  *(resumo — detalhes no README)*

```powershell
# Tudo, sem interação manual
docker compose up --build

# Se já rodou antes, LIMPE os dados antes (ver armadilha §7.4)
docker compose down
Remove-Item -Force .\python\server\data\*.msgpack -ErrorAction SilentlyContinue
Remove-Item -Force .\java\server\data\*.msgpack   -ErrorAction SilentlyContinue
docker compose up --build

# Contrato do PUBLISH (broker + UM servidor rodando)
python tests\test_publish_validation.py --server tcp://localhost:5555 --broker tcp://localhost:5558 --messages-file python\server\data\messages.msgpack
```

Os bots de demonstração aceitam ações pontuais; `ACTION=BOT_LOOP` executa o
comportamento contínuo. Variáveis detalhadas no `README.md`.

---

## 7. Armadilhas que já custaram tempo  *(ESTÁVEL; acrescente as novas)*

1. **PowerShell:** variável de ambiente é `$env:NOME="valor"`. `set NOME=valor` (do CMD) **não** funciona. Variáveis **não atravessam terminais**: um terminal novo começa do zero e usa os **valores padrão** (ex.: publicar no canal `avisos-gerais` em vez do que você configurou).
2. **Diretório atual:** `python client.py` na raiz dá "arquivo não encontrado". Entre na pasta (`cd python\client`).
3. **Estrutura do Java é obrigatória.** O Maven só acha código em `src/main/java/com/vetflow/<server|client>/`, com o **nome do arquivo igual ao da classe** (`Server.java`, não `server.java`; Linux/Docker distinguem maiúsculas). Arquivos soltos na pasta dão `No sources to compile`. Use o `vet_flow.zip` em vez de baixar arquivo por arquivo.
4. **Dados antigos quebram o Compose.** `data/*.msgpack` persiste no host. Se o canal (`cirurgias`, `avisos-clinica`) já existe, o bot de criação sai com erro e os bots que dependem dele (`service_completed_successfully`) **nunca rodam**. Isso é o comportamento certo (duplicado = erro), não bug.
5. **`pom.xml` do client e do server são arquivos diferentes** (`vetflow-client` × `vetflow-server`). Já aconteceu de um sobrescrever o outro ao baixar.
6. **Slow joiner:** em Pub/Sub, publicar logo após assinar perde a mensagem. Em teste manual, a janela do subscriber (`SUBSCRIBE_SECONDS`) tem de **sobrepor** o publish (ex.: 30 s, e publique em outro terminal).
7. **O broker não imprime nada** depois do banner (é só `zmq.proxy`). Silêncio é normal.
8. **O `broker/` fica na raiz**, ao lado de `python/` e `java/` (o Compose usa `build: ./broker`). Já houve cópia perdida em `python/broker/`.
9. **Log do Maven Shade** com `Replacing ... with ...-shaded.jar` e muitos `WARNING` de classes sobrepostas é **normal**; o jar final continua em `target/vetflow-*.jar`.
10. **Arquivo baixado pode chegar vazio.** Se um script "roda e não imprime nada", confira o tamanho. Alternativa: criar o arquivo pelo próprio terminal (*here-string*).
11. **Um servidor por vez na 5555** fora do Docker (os dois usam a mesma porta por padrão).
12. **Bots contínuos no Compose:** `docker compose up --build` permanece ativo; pare com `Ctrl+C` e `docker compose down`.

---

## 8. Pendências, riscos e perguntas em aberto  *(VOLÁTIL)*

**Conhecidos e aceitos (não são bug do enunciado):**

- `PUBLISH_RESPONSE OK` significa **persistido**, não entregue; **não há replay/catch-up** para quem estava offline.
- *Prefix match* do tópico (D16).
- As mensagens de erro do Java expõem nomes internos de classe (Jackson/`ClassCastException`).
- **`LOGIN` e `CHANNEL_CREATE` ainda não têm validação de tipo** (só o `PUBLISH` tem, D19). Hoje o Python guarda um `channel_name` numérico cru e o Java convertia para texto — **inconsistência entre as linguagens, só observada por leitura/teste de comportamento do Python; no Java [LIDO]**. Candidato natural a seguir o mesmo contrato do D19.

**Riscos para a Parte 4 em diante:**

- O heartbeat é síncrono: uma indisponibilidade do serviço de referência pode pausar o servidor no décimo pedido até timeout.
- Ranks e disponibilidade do Reference Service ficam em memória e reiniciam quando o serviço reinicia.
- **Parte 5:** reavaliar D5 (lista regravada × log *append-only*).

**Pendências deste momento:**

- Confirmar `docker compose up --build`, builds Maven e interoperabilidade da Parte 3 em máquina com essas ferramentas.
- Parte 4: eleição de coordenador e sincronização Berkeley.
- Parte 5: replicação dos dados entre servidores.

---

## 9. Quando atualizar este arquivo

| Aconteceu… | Atualize |
|---|---|
| Uma parte foi concluída ou testada | §5 (status + etiqueta de evidência) e §8 |
| Uma decisão nova foi tomada | §4 (**acrescente** linha; se substituir uma antiga, marque a antiga como substituída) |
| Descobriram uma armadilha | §7 |
| Mudou o protocolo | `PROTOCOL.md` **e** §2.3 |
| Mudou a estrutura de pastas/portas | §2.1 / §2.2 |
