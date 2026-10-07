# Protocolo Comum — Python ↔ Java

Este documento é o contrato entre as duas implementações. Qualquer mudança
aqui precisa ser refletida nas duas linguagens.

## Serialização

- Formato: **MessagePack** (binário), tanto para mensagens em rede (ZeroMQ)
  quanto para persistência em disco.
- JSON/XML/texto puro **não são usados** como formato transmitido, conforme
  exigido pelo enunciado.

## Envelope comum

Toda mensagem trocada via ZeroMQ é um mapa MessagePack com esta forma:

```
{
  "type": string,          // identifica o tipo da mensagem (ver tabela abaixo)
  "sender_id": string,     // nome do bot (client) ou identificador do servidor
  "sender_lang": string,   // "python" | "java" — apenas para depuração/testes
                            // de interoperabilidade, não é usado em nenhuma
                            // lógica de negócio
  "timestamp": int64,      // epoch millis (UTC), obrigatório em toda mensagem
  "payload": map           // específico de cada "type"
}
```

`logical_clock` **não** está presente ainda — será adicionado ao envelope
somente quando a Parte 3 (relógios lógicos) for implementada, para não
carregar campo sem uso.

## Convenção de erro/sucesso

Toda mensagem `*_RESPONSE` tem, no `payload`:

- `status`: `"OK"` ou `"ERROR"`
- `error_msg`: string, **obrigatória se `status == "ERROR"`**, ausente caso
  contrário.

## Parte 1 — Login, canais, listagem

Socket ZeroMQ: **REQ (client) / REP (server)**, conforme especificado no
enunciado.

| type | Direção | payload |
|---|---|---|
| `LOGIN_REQUEST` | Client → Server | `{ "bot_name": string }` |
| `LOGIN_RESPONSE` | Server → Client | `{ "status": "OK" }` ou `{ "status": "ERROR", "error_msg": string }` |
| `CHANNEL_CREATE_REQUEST` | Client → Server | `{ "channel_name": string, "bot_name": string }` |
| `CHANNEL_CREATE_RESPONSE` | Server → Client | `{ "status": ..., "error_msg"?: string }` |
| `CHANNEL_LIST_REQUEST` | Client → Server | `{}` |
| `CHANNEL_LIST_RESPONSE` | Server → Client | `{ "channels": [string] }` |
| `ERROR_RESPONSE` | Server → Client | `{ "status": "ERROR", "error_msg": string }` |

`ERROR_RESPONSE` é devolvido quando o servidor não consegue nem
identificar o tipo da requisição — mensagem MessagePack malformada/truncada,
envelope com `type` desconhecido ou ausente, ou campos com tipo
estruturalmente incompatível (ex.: `payload` não é um mapa). Não deve ser
confundido com os `*_RESPONSE` de erro específicos de cada operação (ex.:
`LOGIN_RESPONSE` com `status: ERROR` para `bot_name` ausente), que seguem
sendo usados quando o tipo da requisição foi identificado corretamente mas
um campo esperado está ausente/inválido dentro do fluxo normal daquela
operação.

### Status desta etapa

**Parte 1 completa e validada em Python e Java**: `LOGIN_REQUEST`/`LOGIN_RESPONSE`,
`CHANNEL_CREATE_REQUEST`/`CHANNEL_CREATE_RESPONSE` e
`CHANNEL_LIST_REQUEST`/`CHANNEL_LIST_RESPONSE`, com persistência em disco
(MessagePack) para logins e canais.

Interoperabilidade confirmada nos dois sentidos, para os três tipos de
mensagem: Python client ↔ Python server, Java client ↔ Java server,
Python client ↔ Java server, Java client ↔ Python server. Testado
inclusive o caso mais importante: um canal criado por um bot Java,
persistido por um servidor Python, e lido de volta corretamente por um
client Python.

Robustez validada: mensagens MessagePack malformadas, truncadas, com
`payload`/envelope de tipo estruturalmente incorreto, ou `type`
desconhecido, **não derrubam mais o servidor** — cada mensagem é tratada
isoladamente e qualquer falha de decodificação/processamento vira um
`ERROR_RESPONSE`, mantendo o servidor no ar (corrigido em Python e Java).

Nome de canal duplicado é tratado como erro (`status: ERROR`), comparação
exata case-sensitive — decisão de implementação, não especificada pelo
enunciado.

## Portas (Parte 1)

| Serviço | Porta | Socket |
|---|---|---|
| Python Server | 5555 | REP |
| Java Server | 5555 (container próprio) | REP |

## Parte 2 — Pub/Sub

### Portas

| Serviço | Porta | Socket |
|---|---|---|
| Broker — lado publishers (servidores) | 5557 | `XSUB` (bind) |
| Broker — lado subscribers (clients) | 5558 | `XPUB` (bind) |

Fluxo: `Servers --PUB--> [XSUB broker XPUB] --SUB--> Clients`. O broker
(`broker/broker.py`) não tem lógica de negócio — só `zmq.proxy()`.

### Framing (multipart)

Cada mensagem Pub/Sub é enviada como **2 frames**:

1. **Frame 1 (tópico)**: `channel_name` em UTF-8 puro (ex.: `b"vacinas"`).
   Este é o mecanismo de **roteamento** do ZeroMQ (o broker filtra por
   prefixo de bytes neste frame) — **não é o conteúdo da mensagem**, e
   não precisa (nem deve) ser MessagePack.
2. **Frame 2 (envelope)**: o envelope completo, serializado em
   MessagePack, igual ao padrão REQ/REP da Parte 1.

⚠️ **Limitação conhecida, não corrigida**: o ZeroMQ faz *prefix match* no
tópico, não igualdade exata. Um subscriber de `"vac"` também receberia
mensagens de um canal `"vacinas"`. Aceitável neste projeto desde que os
nomes de canal não compartilhem prefixos ambíguos.

### Tipos de mensagem

| type | Direção | payload |
|---|---|---|
| `PUBLISH_REQUEST` | Client → Server (REQ/REP) | `{ "channel_name": string, "message": string }` |
| `PUBLISH_RESPONSE` | Server → Client (REQ/REP) | `{ "status": ..., "error_msg"?: string }` |
| `CHANNEL_MESSAGE` | Server → Broker → Client (PUB/SUB) | `{ "channel_name": string, "message": string }` |

`PUBLISH_REQUEST` exige que o canal já exista (criado via
`CHANNEL_CREATE_REQUEST` na Parte 1); publicar em canal inexistente
retorna `ERROR`.

### Ordem persistência → publicação

O servidor **persiste a mensagem em disco primeiro, só depois publica**
no broker (nunca o inverso). Motivo: durabilidade antes de visibilidade —
se a publicação falhar (ex.: broker fora do ar), a mensagem não se perde,
só não chega a um subscriber ao vivo naquele instante (não há
replay/catch-up nesta etapa).

### Status desta etapa

**Implementado e testado em Python** (servidor + client reais, ponta a
ponta): `PUBLISH_REQUEST`/`PUBLISH_RESPONSE` via REQ/REP, persistência em
`messages.msgpack`, publicação no broker, e `ACTION=SUBSCRIBE` do client
recebendo a mensagem em tempo real via `SUB` direto no broker. Validado
também o caso de erro (publicar em canal inexistente). Java ainda não
implementado para a Parte 2.