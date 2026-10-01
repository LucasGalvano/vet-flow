package com.vetflow.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java Client (bot) - Parte 1
 *
 * Espelha o comportamento de python/client/client.py: envia UMA
 * requisicao por execucao, definida por ACTION, e exibe a resposta.
 *
 *   ACTION=LOGIN           -> LOGIN_REQUEST
 *   ACTION=CHANNEL_CREATE  -> CHANNEL_CREATE_REQUEST
 *   ACTION=CHANNEL_LIST    -> CHANNEL_LIST_REQUEST
 *
 * O client eh um bot: nao ha interacao manual. Nome do bot, endereco do
 * servidor e a acao vem de variaveis de ambiente.
 */
public class Client {

    private static final ObjectMapper MAPPER = new ObjectMapper(new MessagePackFactory());

    private static final String SERVER_ADDRESS =
            System.getenv().getOrDefault("SERVER_ADDRESS", "tcp://localhost:5555");
    private static final String BOT_NAME =
            System.getenv().getOrDefault("BOT_NAME", "bot-java-1");
    private static final String ACTION =
            System.getenv().getOrDefault("ACTION", "LOGIN").toUpperCase();
    private static final String CHANNEL_NAME =
            System.getenv().getOrDefault("CHANNEL_NAME", "avisos-gerais");

    // Timeout de recepcao: evita que o bot fique bloqueado para sempre
    // caso o servidor nao responda. Importante porque o projeto nao pode
    // depender de interacao manual -- o bot precisa poder desistir
    // sozinho e sinalizar erro via exit code.
    private static final int RECV_TIMEOUT_MS =
            Integer.parseInt(System.getenv().getOrDefault("RECV_TIMEOUT_MS", "5000"));

    public static void main(String[] args) {
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket socket = ctx.createSocket(SocketType.REQ);
            socket.setReceiveTimeOut(RECV_TIMEOUT_MS);
            socket.setLinger(0);
            socket.connect(SERVER_ADDRESS);
            System.out.printf("[CLIENT] '%s' conectando em %s (ACTION=%s)%n", BOT_NAME, SERVER_ADDRESS, ACTION);

            Map<String, Object> request = buildRequest();
            byte[] raw = MAPPER.writeValueAsBytes(request);
            socket.send(raw, 0);
            System.out.println("[SEND] " + request);

            byte[] replyRaw = socket.recv(0);
            if (replyRaw == null) {
                System.out.println("[CLIENT] Timeout: servidor nao respondeu dentro do prazo");
                System.exit(1);
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> response = MAPPER.readValue(replyRaw, Map.class);
            System.out.println("[RECV] " + response);
            printResult(response);
        } catch (Exception e) {
            System.err.println("[CLIENT] Erro: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Map<String, Object> buildEnvelope(String type, Map<String, Object> payload) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("type", type);
        envelope.put("sender_id", BOT_NAME);
        envelope.put("sender_lang", "java");
        envelope.put("timestamp", System.currentTimeMillis());
        envelope.put("payload", payload);
        return envelope;
    }

    private static Map<String, Object> buildRequest() {
        switch (ACTION) {
            case "LOGIN": {
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("bot_name", BOT_NAME);
                return buildEnvelope("LOGIN_REQUEST", payload);
            }
            case "CHANNEL_CREATE": {
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("channel_name", CHANNEL_NAME);
                payload.put("bot_name", BOT_NAME);
                return buildEnvelope("CHANNEL_CREATE_REQUEST", payload);
            }
            case "CHANNEL_LIST": {
                return buildEnvelope("CHANNEL_LIST_REQUEST", new LinkedHashMap<>());
            }
            default:
                System.out.println("[CLIENT] ACTION desconhecida: '" + ACTION + "'");
                System.exit(1);
                return null; // inalcancavel
        }
    }

    @SuppressWarnings("unchecked")
    private static void printResult(Map<String, Object> response) {
        String type = (String) response.get("type");
        Map<String, Object> payload = (Map<String, Object>) response.get("payload");

        if ("CHANNEL_LIST_RESPONSE".equals(type)) {
            Object channels = payload.get("channels");
            System.out.println("[CLIENT] Canais existentes: " + channels);
            return;
        }

        if ("OK".equals(payload.get("status"))) {
            System.out.println("[CLIENT] Acao '" + ACTION + "' concluida com sucesso.");
        } else {
            System.out.println("[CLIENT] Acao '" + ACTION + "' falhou: " + payload.get("error_msg"));
            System.exit(1);
        }
    }
}