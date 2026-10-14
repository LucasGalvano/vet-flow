package com.vetflow.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Bot Java compatível com o cliente Python e com o protocolo MessagePack. */
public class Client {
    private static final ObjectMapper MAPPER = new ObjectMapper(new MessagePackFactory());
    private static final String SERVER_ADDRESS = System.getenv().getOrDefault("SERVER_ADDRESS", "tcp://localhost:5555");
    private static final String BROKER_ADDRESS = System.getenv().getOrDefault("BROKER_ADDRESS", "tcp://localhost:5558");
    private static final String BOT_NAME = System.getenv().getOrDefault("BOT_NAME", "bot-java-1");
    private static final String ACTION = System.getenv().getOrDefault("ACTION", "LOGIN").toUpperCase();
    private static final String CHANNEL_NAME = System.getenv().getOrDefault("CHANNEL_NAME", "avisos-gerais");
    private static final String CHANNELS_ENV = System.getenv().getOrDefault("CHANNELS", CHANNEL_NAME);
    private static final String MESSAGE = System.getenv().getOrDefault("MESSAGE", "mensagem de teste");
    private static final long SUBSCRIBE_MS = (long) (Double.parseDouble(
            System.getenv().getOrDefault("SUBSCRIBE_SECONDS", "5")) * 1000);
    private static final int RECV_TIMEOUT_MS = Integer.parseInt(
            System.getenv().getOrDefault("RECV_TIMEOUT_MS", "5000"));
    private static final long BOT_DELAY_MS = (long) (Double.parseDouble(
            System.getenv().getOrDefault("BOT_DELAY_SECONDS", "1")) * 1000);
    private static long logicalClock = 0;

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            if ("SUBSCRIBE".equals(ACTION)) {
                runSubscribe(context, parseChannels(CHANNELS_ENV), SUBSCRIBE_MS);
            } else if ("BOT".equals(ACTION) || "BOT_LOOP".equals(ACTION)) {
                runBotLoop(context);
            } else {
                runOneShot(context);
            }
        } catch (Exception e) {
            System.err.println("[CLIENT] Erro: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runOneShot(ZContext context) throws Exception {
        try (ZMQ.Socket socket = context.createSocket(SocketType.REQ)) {
            socket.setReceiveTimeOut(RECV_TIMEOUT_MS);
            socket.setLinger(0);
            socket.connect(SERVER_ADDRESS);
            Map<String, Object> request = buildRequest();
            Map<String, Object> response = exchange(socket, request);
            printResult(response);
        }
    }

    private static void runBotLoop(ZContext context) throws Exception {
        try (ZMQ.Socket req = context.createSocket(SocketType.REQ)) {
            req.setReceiveTimeOut(RECV_TIMEOUT_MS);
            req.setLinger(0);
            req.connect(SERVER_ADDRESS);
            Map<String, Object> login = exchange(req,
                    buildEnvelope("LOGIN_REQUEST", Map.of("bot_name", BOT_NAME)));
            if (!isOk(login)) throw new IllegalStateException("login recusado");

            List<String> channels = requestChannels(req);
            String safeName = BOT_NAME.toLowerCase().replaceAll("[^a-z0-9_-]", "-");
            if (safeName.length() > 30) safeName = safeName.substring(0, 30);
            int attempt = 0;
            while (channels.size() < 5 && attempt < 20) {
                String name = "canal-" + safeName + "-" + (attempt + 1);
                Map<String, Object> created = exchange(req, buildEnvelope("CHANNEL_CREATE_REQUEST",
                        Map.of("channel_name", name, "bot_name", BOT_NAME)));
                if (!isOk(created)) {
                    System.out.println("[CLIENT] canal não criado: " + error(created));
                }
                channels = requestChannels(req);
                attempt++;
            }
            if (channels.isEmpty()) throw new IllegalStateException("servidor sem canais");

            List<String> shuffled = new ArrayList<>(channels);
            Collections.shuffle(shuffled);
            List<String> subscribed = shuffled.subList(0, Math.min(3, shuffled.size()));
            Thread subscriber = new Thread(() -> runSubscribe(context, subscribed, -1), "vetflow-subscriber-" + BOT_NAME);
            subscriber.setDaemon(true);
            subscriber.start();

            Random random = new Random();
            while (true) {
                String channel = channels.get(random.nextInt(channels.size()));
                for (int i = 1; i <= 10; i++) {
                    String message = BOT_NAME + ": mensagem " + i + " no canal " + channel;
                    Map<String, Object> response = exchange(req, buildEnvelope("PUBLISH_REQUEST",
                            Map.of("channel_name", channel, "message", message)));
                    if (!isOk(response)) System.out.println("[CLIENT] publicação recusada: " + error(response));
                    Thread.sleep(BOT_DELAY_MS);
                }
            }
        }
    }

    private static List<String> requestChannels(ZMQ.Socket req) throws Exception {
        Map<String, Object> response = exchange(req, buildEnvelope("CHANNEL_LIST_REQUEST", new LinkedHashMap<>()));
        Object raw = ((Map<?, ?>) response.get("payload")).get("channels");
        List<String> channels = new ArrayList<>();
        if (raw instanceof List<?>) {
            for (Object item : (List<?>) raw) if (item instanceof String) channels.add((String) item);
        }
        return channels;
    }

    private static void runSubscribe(ZContext context, List<String> channels, long durationMs) {
        try (ZMQ.Socket sub = context.createSocket(SocketType.SUB)) {
            sub.connect(BROKER_ADDRESS);
            for (String channel : channels) sub.subscribe(channel.getBytes("UTF-8"));
            sub.setReceiveTimeOut(300);
            System.out.println("[CLIENT] '" + BOT_NAME + "' inscrito nos canais " + channels);
            long deadline = durationMs < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + durationMs;
            int received = 0;
            while (System.currentTimeMillis() < deadline) {
                byte[] topic = sub.recv(0);
                if (topic == null) continue;
                byte[] body = sub.hasReceiveMore() ? sub.recv(0) : null;
                if (body == null) continue;
                @SuppressWarnings("unchecked")
                Map<String, Object> envelope = MAPPER.readValue(body, Map.class);
                observeMessage(envelope);
                Map<?, ?> payload = (Map<?, ?>) envelope.get("payload");
                System.out.printf("[PUBSUB-RECV] canal=%s mensagem=%s timestamp_envio=%s "
                                + "timestamp_recebimento=%d logical_clock=%s%n",
                        new String(topic, "UTF-8"), payload.get("message"), envelope.get("timestamp"),
                        System.currentTimeMillis(), envelope.get("logical_clock"));
                received++;
            }
            if (durationMs >= 0) System.out.println("[CLIENT] total de mensagens recebidas: " + received);
        } catch (Exception e) {
            if (durationMs >= 0) System.err.println("[CLIENT] subscriber: " + e.getMessage());
        }
    }

    private static Map<String, Object> exchange(ZMQ.Socket socket, Map<String, Object> request) throws Exception {
        System.out.println("[SEND] " + request);
        socket.send(MAPPER.writeValueAsBytes(request), 0);
        byte[] raw = socket.recv(0);
        if (raw == null) throw new IllegalStateException("servidor não respondeu no prazo");
        @SuppressWarnings("unchecked")
        Map<String, Object> response = MAPPER.readValue(raw, Map.class);
        observeMessage(response);
        System.out.println("[RECV] " + response);
        return response;
    }

    private static synchronized Map<String, Object> buildEnvelope(String type, Map<String, Object> payload) {
        logicalClock++;
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("type", type);
        envelope.put("sender_id", BOT_NAME);
        envelope.put("sender_lang", "java");
        envelope.put("timestamp", System.currentTimeMillis());
        envelope.put("logical_clock", logicalClock);
        envelope.put("payload", payload);
        return envelope;
    }

    private static synchronized void observeMessage(Map<String, Object> envelope) {
        Object value = envelope.get("logical_clock");
        if (value instanceof Number) logicalClock = Math.max(logicalClock, ((Number) value).longValue());
    }

    private static Map<String, Object> buildRequest() {
        Map<String, Object> payload = new LinkedHashMap<>();
        switch (ACTION) {
            case "LOGIN":
                payload.put("bot_name", BOT_NAME);
                return buildEnvelope("LOGIN_REQUEST", payload);
            case "CHANNEL_CREATE":
                payload.put("channel_name", CHANNEL_NAME);
                payload.put("bot_name", BOT_NAME);
                return buildEnvelope("CHANNEL_CREATE_REQUEST", payload);
            case "CHANNEL_LIST":
                return buildEnvelope("CHANNEL_LIST_REQUEST", payload);
            case "PUBLISH":
                payload.put("channel_name", CHANNEL_NAME);
                payload.put("message", MESSAGE);
                return buildEnvelope("PUBLISH_REQUEST", payload);
            default:
                throw new IllegalArgumentException("ACTION desconhecida: " + ACTION);
        }
    }

    private static List<String> parseChannels(String value) {
        List<String> channels = new ArrayList<>();
        for (String channel : value.split(",")) if (!channel.trim().isEmpty()) channels.add(channel.trim());
        return channels;
    }

    private static boolean isOk(Map<String, Object> response) {
        Object payload = response.get("payload");
        return payload instanceof Map<?, ?> && "OK".equals(((Map<?, ?>) payload).get("status"));
    }

    private static String error(Map<String, Object> response) {
        Object payload = response.get("payload");
        return payload instanceof Map<?, ?> ? String.valueOf(((Map<?, ?>) payload).get("error_msg")) : "resposta inválida";
    }

    private static void printResult(Map<String, Object> response) {
        if ("CHANNEL_LIST_RESPONSE".equals(response.get("type"))) {
            System.out.println("[CLIENT] Canais existentes: "
                    + ((Map<?, ?>) response.get("payload")).get("channels"));
        } else if (!isOk(response)) {
            throw new IllegalStateException(error(response));
        } else {
            System.out.println("[CLIENT] ação concluída com sucesso");
        }
    }
}
