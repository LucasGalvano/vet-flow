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

Nome de canal duplicado é tratado como erro (`status: ERROR`), comparação
exata case-sensitive — decisão de implementação, não especificada pelo
enunciado.

## Portas (Parte 1)

| Serviço | Porta | Socket |
|---|---|---|
| Python Server | 5555 | REP |
| Java Server | 5555 (container próprio) | REP |

Reservado para partes futuras (não usado ainda): 5557 (XSUB) / 5558 (XPUB)
para o broker Pub/Sub da Parte 2.