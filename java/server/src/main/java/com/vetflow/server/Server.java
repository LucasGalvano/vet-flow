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

    // Estado em memoria, seguro sem lock porque o loop principal processa
    // uma requisicao REP por vez, sequencialmente (mesma premissa do
    // server Python -- ver nota em Persistence.java).
    private static List<Map<String, Object>> knownLogins = new ArrayList<>();
    private static List<Map<String, Object>> knownChannels = new ArrayList<>();

    public static void main(String[] args) {
        knownLogins = Persistence.loadList(LOGINS_PATH);
        knownChannels = Persistence.loadList(CHANNELS_PATH);
        System.out.printf(
                "[SERVER] Historico carregado: %d login(s) e %d canal(is) previamente persistido(s)%n",
                knownLogins.size(), knownChannels.size());

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket socket = ctx.createSocket(SocketType.REP);
            socket.bind(BIND_ADDRESS);
            System.out.println("[SERVER] Java Server (REP) ouvindo em " + BIND_ADDRESS);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = socket.recv(0);
                if (raw == null) {
                    continue;
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
            }
        } catch (Exception e) {
            System.err.println("[SERVER] Erro fatal: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<String, Object> buildEnvelope(String type, Map<String, Object> payload) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("type", type);
        envelope.put("sender_id", "java-server");
        envelope.put("sender_lang", "java");
        envelope.put("timestamp", System.currentTimeMillis());
        envelope.put("payload", payload);
        return envelope;
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