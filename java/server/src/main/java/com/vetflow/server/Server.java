package com.vetflow.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java Server - Parte 1
 * Sistema distribuido de mensagens - dominio: clinica veterinaria
 *
 * Espelha o comportamento de python/server/server.py, usando o mesmo
 * contrato definido em protocol/PROTOCOL.md:
 *   - Socket ZeroMQ REP.
 *   - LOGIN_REQUEST / LOGIN_RESPONSE, com persistencia.
 *   - CHANNEL_CREATE_REQUEST / CHANNEL_CREATE_RESPONSE, com persistencia.
 *   - CHANNEL_LIST_REQUEST / CHANNEL_LIST_RESPONSE.
 *
 * Nome de canal duplicado e tratado como erro (mesma decisao do lado
 * Python, comparacao exata case-sensitive).
 */
public class Server {

    private static final ObjectMapper MAPPER = new ObjectMapper(new MessagePackFactory());

    private static final String BIND_ADDRESS =
            System.getenv().getOrDefault("SERVER_BIND_ADDRESS", "tcp://*:5555");

    private static final String LOGINS_PATH =
            System.getenv().getOrDefault("LOGINS_PERSISTENCE_PATH", Persistence.DEFAULT_LOGINS_PATH);

    private static final String CHANNELS_PATH =
            System.getenv().getOrDefault("CHANNELS_PERSISTENCE_PATH", Persistence.DEFAULT_CHANNELS_PATH);

    private static final String MESSAGES_PATH =
            System.getenv().getOrDefault("MESSAGES_PERSISTENCE_PATH", Persistence.DEFAULT_MESSAGES_PATH);

    // Endereco do XSUB do broker (Parte 2) -- o servidor conecta um
    // socket PUB nele para publicar mensagens. Processo separado, ver
    // broker/broker.py.
    private static final String BROKER_XSUB_ADDRESS =
            System.getenv().getOrDefault("BROKER_XSUB_ADDRESS", "tcp://localhost:5557");
    private static final String REFERENCE_ADDRESS =
            System.getenv().getOrDefault("REFERENCE_ADDRESS", "tcp://localhost:5559");
    private static final String SERVER_NAME =
            System.getenv().getOrDefault("SERVER_NAME", "java-server");
    private static final int HEARTBEAT_EVERY =
            Integer.parseInt(System.getenv().getOrDefault("HEARTBEAT_EVERY", "10"));

    // Resiliencia frente a um Reference Service indisponivel (ver CONTEXTO.md, D24):
    //  - timeout de cada troca com o Reference (antes fixo em 5000 ms);
    //  - tentativas explicitas de registro, com intervalo inicial e backoff
    //    exponencial limitado (o intervalo dobra a cada falha ate o maximo).
    private static final int REFERENCE_TIMEOUT_MS = Math.max(1,
            Integer.parseInt(System.getenv().getOrDefault("REFERENCE_TIMEOUT_MS", "1000")));
    private static final double REFERENCE_RETRY_INITIAL_SECONDS = Math.max(0.05,
            Double.parseDouble(System.getenv().getOrDefault("REFERENCE_RETRY_INITIAL_SECONDS", "1")));
    private static final double REFERENCE_RETRY_MAX_SECONDS = Math.max(REFERENCE_RETRY_INITIAL_SECONDS,
            Double.parseDouble(System.getenv().getOrDefault("REFERENCE_RETRY_MAX_SECONDS", "10")));
    // O recv do REP "acorda" a cada IDLE_TICK_MS para o loop poder tentar o
    // registro mesmo sem trafego de clients (sem threads).
    private static final int IDLE_TICK_MS = 500;

    // Estado em memoria, seguro sem lock porque o loop principal processa
    // uma requisicao REP por vez, sequencialmente (mesma premissa do
    // server Python -- ver nota em Persistence.java).
    private static List<Map<String, Object>> knownLogins = new ArrayList<>();
    private static List<Map<String, Object>> knownChannels = new ArrayList<>();

    // Socket PUB, criado em main() e usado pelo handler de PUBLISH_REQUEST.
    private static ZMQ.Socket pubSocket;
    private static ZMQ.Socket referenceSocket;
    private static long logicalClock = 0;
    private static long clockOffsetMs = 0;
    private static long clientMessagesReceived = 0;
    private static Object serverRank;
    private static ZContext zmqContext;
    private static boolean referenceRegistered = false;
    private static double registrationRetryDelay = REFERENCE_RETRY_INITIAL_SECONDS;
    private static long nextRegistrationAttemptMs = 0;

    public static void main(String[] args) {
        knownLogins = Persistence.loadList(LOGINS_PATH);
        knownChannels = Persistence.loadList(CHANNELS_PATH);
        List<Map<String, Object>> knownMessages = Persistence.loadList(MESSAGES_PATH);
        System.out.printf(
                "[SERVER] Historico carregado: %d login(s), %d canal(is) e %d mensagem(ns) previamente persistido(s)%n",
                knownLogins.size(), knownChannels.size(), knownMessages.size());

        try (ZContext ctx = new ZContext()) {
            zmqContext = ctx;
            ZMQ.Socket socket = ctx.createSocket(SocketType.REP);
            socket.setReceiveTimeOut(IDLE_TICK_MS);
            socket.bind(BIND_ADDRESS);
            System.out.println("[SERVER] Java Server (REP) ouvindo em " + BIND_ADDRESS);

            // PUB conectado ao broker (Parte 2). "connect", nao "bind":
            // quem faz bind e o broker (XSUB), o servidor so se conecta.
            pubSocket = ctx.createSocket(SocketType.PUB);
            pubSocket.connect(BROKER_XSUB_ADDRESS);
            System.out.println("[SERVER] PUB conectado ao broker em " + BROKER_XSUB_ADDRESS);

            // O socket do Reference e criado SOB DEMANDA (ver referenceRequest):
            // criar/conectar pode falhar (ex.: hostname do Reference sem
            // resolucao no Docker) e isso deve contar como "Reference
            // indisponivel", nao como erro fatal.
            // O registro inicial NAO e mais fatal: se o Reference estiver fora
            // do ar, o servidor sobe mesmo assim e tenta de novo (backoff) a
            // partir do loop principal.
            if (!tryRegister()) {
                System.out.println("[SERVER] iniciado SEM registro no Reference Service; "
                        + "novas tentativas automaticas em andamento");
            }

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = socket.recv(0);
                if (raw == null) {
                    maybeRegister(); // ocioso: aproveita para tentar o registro
                    continue;
                }
                clientMessagesReceived++;
                if (clientMessagesReceived % HEARTBEAT_EVERY == 0) {
                    sendHeartbeat();
                }

                // Cada mensagem e tratada isoladamente: qualquer falha ao
                // decodificar o MessagePack, ou ao processar o envelope
                // (campos ausentes, tipos inesperados, envelope que nao e
                // um Map, etc.), e capturada aqui e vira um
                // ERROR_RESPONSE, em vez de propagar e encerrar o
                // servidor. O socket REP exige exatamente 1 send() por
                // recv(), entao SEMPRE respondemos algo, mesmo em erro.
                Map<String, Object> response;
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> envelope = MAPPER.readValue(raw, Map.class);
                    observeMessage(envelope);
                    System.out.println("[RECV] " + envelope);
                    response = dispatch(envelope);
                } catch (Exception e) {
                    System.err.println("[ERROR] Falha ao processar mensagem: " + e.getMessage());
                    response = errorResponse(
                            "ERROR_RESPONSE",
                            "mensagem invalida ou mal-formada: " + e.getMessage());
                }

                byte[] out = MAPPER.writeValueAsBytes(response);
                socket.send(out, 0);
                System.out.println("[SEND] " + response);
                maybeRegister(); // depois de responder: a tentativa nao atrasa o client
            }
        } catch (Exception e) {
            System.err.println("[SERVER] Erro fatal: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<String, Object> buildEnvelope(String type, Map<String, Object> payload) {
        logicalClock++;
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("type", type);
        envelope.put("sender_id", "java-server");
        envelope.put("sender_lang", "java");
        envelope.put("timestamp", System.currentTimeMillis() + clockOffsetMs);
        envelope.put("logical_clock", logicalClock);
        envelope.put("payload", payload);
        return envelope;
    }

    private static void observeMessage(Map<String, Object> envelope) {
        Object value = envelope.get("logical_clock");
        if (value instanceof Number) {
            logicalClock = Math.max(logicalClock, ((Number) value).longValue());
        }
    }

    @SuppressWarnings("unchecked")
    private static ZMQ.Socket newReferenceSocket() {
        ZMQ.Socket sock = zmqContext.createSocket(SocketType.REQ);
        sock.setReceiveTimeOut(REFERENCE_TIMEOUT_MS);
        sock.setSendTimeOut(REFERENCE_TIMEOUT_MS);
        sock.setLinger(0);
        sock.connect(REFERENCE_ADDRESS);
        return sock;
    }

    /**
     * Descarta e recria o socket REQ do Reference.
     *
     * Um REQ que estourou o timeout fica preso em "aguardando resposta": todo
     * send seguinte falha e ele nunca se recupera sozinho. Recriar o socket
     * (linger 0 descarta a requisicao pendente) e a forma padrao de sair
     * desse estado.
     */
    private static void resetReferenceSocket() {
        try {
            if (referenceSocket != null) {
                referenceSocket.close(); // (ZContext tambem remove o socket da sua lista)
            }
        } catch (Exception ignored) {
            // melhor esforco
        }
        // Recriado sob demanda por referenceRequest(): se criar falhar agora
        // (ex.: DNS do Reference fora do ar), a excecao nao pode escapar daqui.
        referenceSocket = null;
    }

    private static long nowMs() {
        return System.nanoTime() / 1_000_000L;
    }

    /** Marca o servidor como nao registrado e agenda a proxima tentativa. */
    private static void scheduleRegistrationRetry(String reason) {
        referenceRegistered = false;
        nextRegistrationAttemptMs = nowMs() + (long) (registrationRetryDelay * 1000);
        System.out.printf("[REFERENCE] indisponivel (%s); nova tentativa de registro em %ss%n",
                reason, formatSeconds(registrationRetryDelay));
        registrationRetryDelay = Math.min(registrationRetryDelay * 2, REFERENCE_RETRY_MAX_SECONDS);
    }

    private static String formatSeconds(double seconds) {
        return seconds == Math.rint(seconds) ? String.valueOf((long) seconds) : String.valueOf(seconds);
    }

    /** Tenta registrar no Reference. Nunca lanca: em falha, agenda a proxima tentativa (backoff). */
    private static boolean tryRegister() {
        try {
            Map<String, Object> registration = referenceRequest(
                    "REFERENCE_REGISTER_REQUEST", Map.of("server_name", SERVER_NAME));
            serverRank = registration.get("rank");
        } catch (Exception e) {
            scheduleRegistrationRetry(String.valueOf(e.getMessage()));
            return false;
        }
        referenceRegistered = true;
        registrationRetryDelay = REFERENCE_RETRY_INITIAL_SECONDS;
        System.out.printf("[SERVER] registrado como %s, rank=%s, hora ajustada em %dms%n",
                SERVER_NAME, serverRank, clockOffsetMs);
        return true;
    }

    /** Chamado pelo loop principal: se nao ha registro e ja passou a hora da proxima tentativa, tenta. */
    private static void maybeRegister() {
        if (!referenceRegistered && nowMs() >= nextRegistrationAttemptMs) {
            tryRegister();
        }
    }

    private static Map<String, Object> referenceRequest(String type, Map<String, Object> payload) throws Exception {
        Map<String, Object> request = buildEnvelope(type, payload);
        byte[] raw;
        try {
            if (referenceSocket == null) {
                referenceSocket = newReferenceSocket(); // pode lancar (DNS etc.)
            }
            if (!referenceSocket.send(MAPPER.writeValueAsBytes(request), 0)) {
                throw new IllegalStateException("Reference Service: falha ao enviar");
            }
            raw = referenceSocket.recv(0);
        } catch (RuntimeException e) {
            resetReferenceSocket();
            throw e;
        }
        if (raw == null) {
            resetReferenceSocket();
            throw new IllegalStateException("Reference Service sem resposta");
        }
        Map<String, Object> response = MAPPER.readValue(raw, Map.class);
        observeMessage(response);
        Map<String, Object> result = (Map<String, Object>) response.get("payload");
        if (result == null || !"OK".equals(result.get("status"))) {
            throw new IllegalStateException("Reference Service: " + (result == null ? "resposta inválida" : result.get("error_msg")));
        }
        Object referenceTime = result.get("reference_time_ms");
        if (referenceTime instanceof Number) {
            clockOffsetMs = ((Number) referenceTime).longValue() - System.currentTimeMillis();
        }
        return result;
    }

    private static void sendHeartbeat() {
        if (!referenceRegistered) {
            return; // sem registro, quem cuida disso e maybeRegister()
        }
        try {
            Map<String, Object> response = referenceRequest(
                    "REFERENCE_HEARTBEAT_REQUEST", Map.of("server_name", SERVER_NAME));
            System.out.printf("[HEARTBEAT] servidor=%s rank=%s offset=%dms%n",
                    SERVER_NAME, response.get("rank"), clockOffsetMs);
        } catch (Exception e) {
            System.err.println("[HEARTBEAT] falha ao sincronizar com referência: " + e.getMessage());
            scheduleRegistrationRetry(String.valueOf(e.getMessage()));
        }
    }

    private static Map<String, Object> dispatch(Map<String, Object> envelope) {
        String type = (String) envelope.get("type");

        if ("LOGIN_REQUEST".equals(type)) {
            return handleLoginRequest(envelope);
        }
        if ("CHANNEL_CREATE_REQUEST".equals(type)) {
            return handleChannelCreateRequest(envelope);
        }
        if ("CHANNEL_LIST_REQUEST".equals(type)) {
            return handleChannelListRequest();
        }
        if ("PUBLISH_REQUEST".equals(type)) {
            return handlePublishRequest(envelope);
        }

        // Tipo desconhecido nao deve derrubar o servidor (REP exige sempre
        // 1 send() por recv(), senao o socket trava em estado inconsistente).
        return errorResponse("ERROR_RESPONSE", "tipo de mensagem desconhecido: " + type);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> handleLoginRequest(Map<String, Object> envelope) {
        Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
        String botName = stringOrNull(payload, "bot_name");

        if (botName == null || botName.isEmpty()) {
            return errorResponse("LOGIN_RESPONSE", "campo 'bot_name' ausente ou vazio");
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("bot_name", botName);
        record.put("sender_lang", envelope.get("sender_lang"));
        record.put("timestamp", envelope.get("timestamp"));

        knownLogins = Persistence.appendItem(record, LOGINS_PATH);

        System.out.printf(
                "[LOGIN] bot='%s' lang=%s timestamp=%s (total de logins persistidos: %d)%n",
                botName, envelope.get("sender_lang"), envelope.get("timestamp"), knownLogins.size());

        return okResponse("LOGIN_RESPONSE");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> handleChannelCreateRequest(Map<String, Object> envelope) {
        Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
        String channelName = stringOrNull(payload, "channel_name");
        String botName = stringOrNull(payload, "bot_name");

        if (channelName == null || channelName.isEmpty()) {
            return errorResponse("CHANNEL_CREATE_RESPONSE", "campo 'channel_name' ausente ou vazio");
        }
        if (botName == null || botName.isEmpty()) {
            return errorResponse("CHANNEL_CREATE_RESPONSE", "campo 'bot_name' ausente ou vazio");
        }

        boolean alreadyExists = knownChannels.stream()
                .anyMatch(c -> channelName.equals(c.get("channel_name")));
        if (alreadyExists) {
            return errorResponse("CHANNEL_CREATE_RESPONSE", "canal '" + channelName + "' ja existe");
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("channel_name", channelName);
        record.put("created_by", botName);
        record.put("timestamp", envelope.get("timestamp"));

        knownChannels = Persistence.appendItem(record, CHANNELS_PATH);

        System.out.printf(
                "[CHANNEL_CREATE] channel='%s' created_by='%s' (total de canais: %d)%n",
                channelName, botName, knownChannels.size());

        return okResponse("CHANNEL_CREATE_RESPONSE");
    }

    private static Map<String, Object> handleChannelListRequest() {
        List<Object> channelNames = new ArrayList<>();
        for (Map<String, Object> c : knownChannels) {
            channelNames.add(c.get("channel_name"));
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("channels", channelNames);
        return buildEnvelope("CHANNEL_LIST_RESPONSE", payload);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> handlePublishRequest(Map<String, Object> envelope) {
        Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");

        // Nenhuma requisicao invalida chega a persistencia nem ao broker:
        // toda validacao acontece antes do primeiro appendItem.
        for (String field : new String[] {"channel_name", "message"}) {
            String error = validateRequiredString(payload, field);
            if (error != null) {
                return errorResponse("PUBLISH_RESPONSE", error);
            }
        }

        String channelName = (String) payload.get("channel_name");
        String message = (String) payload.get("message");

        boolean channelExists = knownChannels.stream()
                .anyMatch(c -> channelName.equals(c.get("channel_name")));
        if (!channelExists) {
            return errorResponse("PUBLISH_RESPONSE", "canal '" + channelName + "' nao existe");
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("channel_name", channelName);
        record.put("sender_id", envelope.get("sender_id"));
        record.put("sender_lang", envelope.get("sender_lang"));
        record.put("message", message);
        record.put("timestamp", envelope.get("timestamp"));

        // 1) Persiste primeiro (durabilidade antes de visibilidade --
        // ver ordem documentada em protocol/PROTOCOL.md).
        Persistence.appendItem(record, MESSAGES_PATH);

        // 2) So entao publica no broker, para subscribers ao vivo.
        Map<String, Object> publishPayload = new LinkedHashMap<>();
        publishPayload.put("channel_name", channelName);
        publishPayload.put("message", message);
        Map<String, Object> publishEnvelope = buildEnvelope("CHANNEL_MESSAGE", publishPayload);

        try {
            byte[] topic = channelName.getBytes("UTF-8");
            byte[] body = MAPPER.writeValueAsBytes(publishEnvelope);
            pubSocket.sendMore(topic);
            pubSocket.send(body, 0);
        } catch (Exception e) {
            // Nao deve acontecer em condicoes normais; se acontecer, a
            // mensagem ja esta persistida (passo 1), entao nao perdemos
            // dado -- so o subscriber ao vivo que nao recebe.
            System.err.println("[ERROR] Falha ao publicar no broker: " + e.getMessage());
        }

        System.out.printf("[PUBLISH] channel='%s' message='%s'%n", channelName, message);

        return okResponse("PUBLISH_RESPONSE");
    }

    /**
     * Contrato de campo string obrigatorio (ver PROTOCOL.md, PUBLISH_REQUEST).
     * Retorna null se valido, ou a mensagem de erro. SEM conversao implicita:
     *   - ausente, null ou string vazia -> "campo 'X' ausente ou vazio"
     *   - qualquer outro tipo (numero, booleano, lista, objeto) ->
     *     "campo 'X' deve ser uma string"
     * Mesmas mensagens e mesma ordem de verificacao do server.py.
     */
    private static String validateRequiredString(Map<String, Object> payload, String key) {
        Object value = payload != null ? payload.get(key) : null;
        if (value == null) {
            return "campo '" + key + "' ausente ou vazio";
        }
        if (!(value instanceof String)) {
            return "campo '" + key + "' deve ser uma string";
        }
        if (((String) value).isEmpty()) {
            return "campo '" + key + "' ausente ou vazio";
        }
        return null;
    }

    private static String stringOrNull(Map<String, Object> payload, String key) {
        if (payload == null) {
            return null;
        }
        Object value = payload.get(key);
        return value != null ? value.toString() : null;
    }

    private static Map<String, Object> okResponse(String type) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", "OK");
        return buildEnvelope(type, payload);
    }

    private static Map<String, Object> errorResponse(String type, String errorMsg) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", "ERROR");
        payload.put("error_msg", errorMsg);
        return buildEnvelope(type, payload);
    }
}
