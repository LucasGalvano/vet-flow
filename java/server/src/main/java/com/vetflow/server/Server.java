package com.vetflow.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
    // Backend do broker REQ/REP (ROUTER/DEALER). Opcional: se definido, o REP tambem
    // se CONECTA ao broker (alem do bind) e passa a receber requisicoes em round-robin.
    private static final String BROKER_BACKEND_ADDRESS =
            System.getenv().getOrDefault("SERVER_BROKER_ADDRESS", "");
    private static final int HEARTBEAT_EVERY =
            Integer.parseInt(System.getenv().getOrDefault("HEARTBEAT_EVERY", "10"));

    // --- Parte 4: eleicao (bully) e sincronizacao de relogio com o coordenador ---
    // Mensagens de clientes entre duas sincronizacoes com o coordenador.
    private static final int SYNC_EVERY =
            Integer.parseInt(System.getenv().getOrDefault("SYNC_EVERY", "15"));
    // XPUB do broker Pub/Sub: o servidor assina o topico `servers` para saber quem e o coordenador.
    private static final String BROKER_XPUB_ADDRESS =
            System.getenv().getOrDefault("BROKER_XPUB_ADDRESS", "tcp://localhost:5558");
    // Porta em que os servidores atendem uns aos outros (a mesma do REP: 5555). O endereco de
    // um par e tcp://<nome do servidor>:PEER_PORT (o nome registrado no Reference e o hostname).
    private static final int PEER_PORT =
            Integer.parseInt(System.getenv().getOrDefault("PEER_PORT", "5555"));
    private static final int PEER_TIMEOUT_MS = Math.max(1,
            Integer.parseInt(System.getenv().getOrDefault("PEER_TIMEOUT_MS", "1000")));
    // Quanto esperar o anuncio do coordenador depois que um servidor de rank maior respondeu OK.
    private static final double ELECTION_WAIT_SECONDS =
            Double.parseDouble(System.getenv().getOrDefault("ELECTION_WAIT_SECONDS", "5"));
    // O coordenador republica o anuncio periodicamente: o Pub/Sub nao garante entrega a quem
    // acabou de assinar (slow joiner) e isso tambem corrige coordenadores divergentes.
    private static final double ANNOUNCE_EVERY_SECONDS =
            Double.parseDouble(System.getenv().getOrDefault("COORDINATOR_ANNOUNCE_SECONDS", "5"));
    // Desvio artificial do relogio local (para demonstrar a sincronizacao: no Docker todos os
    // containers compartilham o mesmo relogio fisico).
    private static final long CLOCK_SKEW_MS =
            Long.parseLong(System.getenv().getOrDefault("CLOCK_SKEW_MS", "0"));
    private static final String SERVERS_TOPIC = "servers";

    // --- Parte 5: replicacao dos dados entre todos os servidores ---
    // Topico Pub/Sub em que cada servidor publica os registros que grava (todos assinam).
    private static final String REPLICATION_TOPIC = "replication";
    // Intervalo (com variacao aleatoria de +-30%) da anti-entropia periodica com um par.
    private static final double REPL_SYNC_SECONDS =
            Double.parseDouble(System.getenv().getOrDefault("REPL_SYNC_SECONDS", "10"));
    // Maximo de registros por SYNC_RESPONSE e de lotes por sincronizacao.
    private static final int REPL_BATCH = Math.max(1,
            Integer.parseInt(System.getenv().getOrDefault("REPL_BATCH", "500")));
    private static final int REPL_MAX_BATCHES = 20;
    private static final List<String> KINDS = List.of("login", "channel", "message");

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
    private static List<Map<String, Object>> knownMessages = new ArrayList<>();
    // Parte 5
    private static final Map<String, Map<String, Object>> channelWinners = new HashMap<>(); // nome -> registro vencedor
    private static final Map<String, Set<Long>> replSeen = new HashMap<>();   // origem -> seq ja aplicados
    private static final Map<String, Long> replContiguous = new HashMap<>();  // origem -> maior n com 1..n aplicados
    private static long nextSeq = 1;                                          // proximo seq deste servidor
    private static final Set<String> dirtyKinds = new LinkedHashSet<>();      // replicados ainda nao gravados
    private static final Set<String> replGapPeers = new LinkedHashSet<>();    // origens com buraco detectado
    private static boolean replInitialSyncDone = false;
    private static long replNextAttemptMs = 0;
    private static long nextReplSyncAtMs = 0;
    private static int replPeerCursor = 0;
    private static final Random RANDOM = new Random();
    private static ZMQ.Socket repSocket; // REP principal (tambem atende pares enquanto se espera resposta)
    private static long nextHeartbeatAt = HEARTBEAT_EVERY;
    private static long nextSyncCount = SYNC_EVERY;

    // Socket PUB, criado em main() e usado pelo handler de PUBLISH_REQUEST.
    private static ZMQ.Socket pubSocket;
    private static ZMQ.Socket referenceSocket;
    // Maior contador remoto aceito: Long.MAX_VALUE - 1, deixando folga para o +1
    // do proximo envio. Valores fora de [0, MAX_LOGICAL_CLOCK] (negativos, ou
    // >= 2^63) e tipos nao inteiros (double, boolean) sao IGNORADOS em vez de
    // adotados: antes, 2^63-1 fazia o contador estourar para Long.MIN_VALUE e
    // o relogio logico andava para tras. Mesma regra do server.py.
    private static final long MAX_LOGICAL_CLOCK = Long.MAX_VALUE - 1;
    private static long logicalClock = 0;
    private static long clockAdjustMs = 0; // ajuste acumulado pela sincronizacao com o coordenador
    // Parte 4
    private static String coordinator = null; // nome do coordenador eleito (null = desconhecido)
    private static boolean electionDue = false;
    private static long electionNotBeforeMs = 0;
    private static boolean awaitingAnnouncement = false;
    private static long awaitingAnnouncementUntilMs = 0;
    private static boolean announceDue = false;
    private static long nextAnnouncementAtMs = 0;
    private static boolean syncDue = false;
    private static ZMQ.Socket subSocket;
    private static long clientMessagesReceived = 0;
    private static Object serverRank;
    private static ZContext zmqContext;
    private static boolean referenceRegistered = false;
    private static double registrationRetryDelay = REFERENCE_RETRY_INITIAL_SECONDS;
    private static long nextRegistrationAttemptMs = 0;

    public static void main(String[] args) {
        // Os servidores se enderecam pelo nome (DNS do Compose). Um par que cai e volta pode
        // mudar de IP: nada de cache longo de resolucao (nem de falhas) na JVM.
        java.security.Security.setProperty("networkaddress.cache.ttl", "5");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
        loadState();
        System.out.printf(
                "[SERVER] Historico carregado: %d login(s), %d canal(is) e %d mensagem(ns) previamente persistido(s)%n",
                knownLogins.size(), channelWinners.size(), knownMessages.size());

        try (ZContext ctx = new ZContext()) {
            zmqContext = ctx;
            ZMQ.Socket socket = ctx.createSocket(SocketType.REP);
            socket.setReceiveTimeOut(IDLE_TICK_MS);
            socket.bind(BIND_ADDRESS);
            repSocket = socket;
            System.out.println("[SERVER] Java Server (REP) ouvindo em " + BIND_ADDRESS);
            if (!BROKER_BACKEND_ADDRESS.isEmpty()) {
                socket.connect(BROKER_BACKEND_ADDRESS);
                System.out.println("[SERVER] REP tambem conectado ao broker REQ/REP em "
                        + BROKER_BACKEND_ADDRESS);
            }

            // PUB conectado ao broker (Parte 2). "connect", nao "bind":
            // quem faz bind e o broker (XSUB), o servidor so se conecta.
            pubSocket = ctx.createSocket(SocketType.PUB);
            pubSocket.connect(BROKER_XSUB_ADDRESS);
            System.out.println("[SERVER] PUB conectado ao broker em " + BROKER_XSUB_ADDRESS);

            // SUB no XPUB do broker (Parte 4): recebe o anuncio do coordenador no topico `servers`.
            subSocket = ctx.createSocket(SocketType.SUB);
            subSocket.connect(BROKER_XPUB_ADDRESS);
            subSocket.subscribe(SERVERS_TOPIC.getBytes(StandardCharsets.UTF_8));
            subSocket.subscribe(REPLICATION_TOPIC.getBytes(StandardCharsets.UTF_8));
            System.out.println("[SERVER] SUB conectado ao broker em " + BROKER_XPUB_ADDRESS
                    + ", assinando os topicos '" + SERVERS_TOPIC + "' e '" + REPLICATION_TOPIC + "'");

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
                drainAnnouncements();
                byte[] raw = socket.recv(0);
                if (raw == null) {
                    housekeeping(); // ocioso: registro, eleicao, reanuncio e sincronizacao
                    continue;
                }

                // Cada mensagem e tratada por processRaw(): qualquer falha vira um
                // ERROR_RESPONSE (o socket REP exige exatamente 1 send() por recv()).
                boolean peerMessage = processRaw(raw);

                // Depois de responder: nada abaixo atrasa o cliente que acabou de ser atendido.
                if (!peerMessage) {
                    afterClientMessage();
                }
                housekeeping();
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
        envelope.put("sender_id", SERVER_NAME);
        envelope.put("sender_lang", "java");
        envelope.put("timestamp", physicalNowMs());
        envelope.put("logical_clock", logicalClock);
        envelope.put("payload", payload);
        return envelope;
    }

    private static void observeMessage(Map<String, Object> envelope) {
        Object value = envelope.get("logical_clock");
        // So inteiros de ate 64 bits com sinal. BigInteger (uint64 > Long.MAX),
        // Double e Boolean caem fora e sao ignorados.
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            long received = ((Number) value).longValue();
            if (received >= 0 && received <= MAX_LOGICAL_CLOCK) {
                logicalClock = Math.max(logicalClock, received);
            }
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
        System.out.printf("[SERVER] registrado como %s, rank=%s%n", SERVER_NAME, serverRank);
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
        System.out.println("[REF-SEND] " + request);
        byte[] raw;
        try {
            if (referenceSocket == null) {
                referenceSocket = newReferenceSocket(); // pode lancar (DNS etc.)
            }
            if (!referenceSocket.send(MAPPER.writeValueAsBytes(request), 0)) {
                throw new IllegalStateException("Reference Service: falha ao enviar");
            }
            raw = recvWhileServing(referenceSocket, REFERENCE_TIMEOUT_MS);
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
        System.out.println("[REF-RECV] " + response);
        Map<String, Object> result = (Map<String, Object>) response.get("payload");
        if (result == null || !"OK".equals(result.get("status"))) {
            throw new IllegalStateException("Reference Service: " + (result == null ? "resposta inválida" : result.get("error_msg")));
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
            System.out.printf("[HEARTBEAT] servidor=%s rank=%s%n", SERVER_NAME, response.get("rank"));
        } catch (Exception e) {
            System.err.println("[HEARTBEAT] falha ao sincronizar com referência: " + e.getMessage());
            scheduleRegistrationRetry(String.valueOf(e.getMessage()));
        }
    }

    // ------------------------------------------------------------------
    // Parte 4: eleicao (bully) e sincronizacao de relogio com o coordenador
    //
    // Tudo roda no loop unico do REP (sem threads), como o resto do servidor:
    //  - ELECTION_REQUEST / CLOCK_REQUEST chegam no mesmo REP (porta 5555) e sao
    //    tratados por dispatch(); o servidor so inicia trocas com servidores de rank
    //    MAIOR (eleicao) ou com o coordenador (relogio), entao as esperas formam uma
    //    cadeia, nao um ciclo.
    //  - O anuncio do coordenador chega pelo topico `servers` (socket SUB no broker).
    // ------------------------------------------------------------------

    private static boolean isPeerType(Object type) {
        return "ELECTION_REQUEST".equals(type) || "CLOCK_REQUEST".equals(type) || "SYNC_REQUEST".equals(type);
    }

    /** Relogio local deste servidor (com o desvio simulado CLOCK_SKEW_MS). */
    private static long localClockMs() {
        return System.currentTimeMillis() + CLOCK_SKEW_MS;
    }

    /** Hora fisica usada nos timestamps: relogio local + ajuste do coordenador. */
    private static long physicalNowMs() {
        return localClockMs() + clockAdjustMs;
    }

    private static Integer myRank() {
        return serverRank instanceof Number ? Integer.valueOf(((Number) serverRank).intValue()) : null;
    }

    /** REQ/REP direto com outro servidor. Socket descartavel por troca (um REQ que estoura o timeout fica inutilizavel). */
    private static Map<String, Object> peerRequest(String name, String type, Map<String, Object> payload)
            throws Exception {
        return peerRequest(name, type, payload, true);
    }

    private static Map<String, Object> peerRequest(String name, String type, Map<String, Object> payload,
            boolean verbose) throws Exception {
        ZMQ.Socket sock = null;
        try {
            sock = zmqContext.createSocket(SocketType.REQ);
            sock.setReceiveTimeOut(PEER_TIMEOUT_MS);
            sock.setSendTimeOut(PEER_TIMEOUT_MS);
            sock.setLinger(0);
            sock.connect("tcp://" + name + ":" + PEER_PORT);
            Map<String, Object> request = buildEnvelope(type, payload);
            if (verbose) {
                System.out.println("[PEER-SEND] para=" + name + " " + request);
            }
            if (!sock.send(MAPPER.writeValueAsBytes(request), 0)) {
                throw new IllegalStateException("falha ao enviar para " + name);
            }
            byte[] raw = recvWhileServing(sock, PEER_TIMEOUT_MS);
            if (raw == null) {
                throw new IllegalStateException(name + " sem resposta no prazo");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> response = MAPPER.readValue(raw, Map.class);
            observeMessage(response);
            if (verbose) {
                System.out.println("[PEER-RECV] de=" + name + " " + response);
            }
            return response;
        } finally {
            if (sock != null) {
                sock.close();
            }
        }
    }

    /** Servidores disponiveis segundo o Reference, exceto este. */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> otherServers() throws Exception {
        Map<String, Object> result = referenceRequest("REFERENCE_LIST_REQUEST", new LinkedHashMap<>());
        List<Map<String, Object>> servers = new ArrayList<>();
        Object raw = result.get("servers");
        if (raw instanceof List<?>) {
            for (Object item : (List<?>) raw) {
                if (item instanceof Map<?, ?>) {
                    Map<String, Object> server = (Map<String, Object>) item;
                    if (!SERVER_NAME.equals(server.get("name"))) {
                        servers.add(server);
                    }
                }
            }
        }
        return servers;
    }

    /** Publica no topico `servers` o nome do coordenador. */
    private static void announceCoordinator() {
        announceDue = false;
        nextAnnouncementAtMs = nowMs() + (long) (ANNOUNCE_EVERY_SECONDS * 1000);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("coordinator", SERVER_NAME);
        payload.put("rank", serverRank);
        Map<String, Object> envelope = buildEnvelope("COORDINATOR_ANNOUNCEMENT", payload);
        try {
            byte[] body = MAPPER.writeValueAsBytes(envelope);
            System.out.println("[PUB-SEND] topic=" + SERVERS_TOPIC + " " + envelope);
            pubSocket.sendMore(SERVERS_TOPIC.getBytes(StandardCharsets.UTF_8));
            pubSocket.send(body, 0);
        } catch (Exception e) {
            System.err.println("[ERROR] Falha ao anunciar o coordenador: " + e.getMessage());
        }
    }

    private static void becomeCoordinator() {
        coordinator = SERVER_NAME;
        awaitingAnnouncement = false;
        System.out.println("[COORDENADOR] " + SERVER_NAME + " (rank " + serverRank + ") eleito coordenador");
        announceCoordinator();
    }

    /**
     * Eleicao pelo algoritmo do valentao (bully): pede eleicao aos servidores de rank maior.
     * Se algum responde OK, espera o anuncio dele; se nenhum responde, este servidor e o
     * novo coordenador.
     */
    private static void runElection() {
        electionDue = false;
        Integer rank = myRank();
        if (rank == null) {
            electionDue = true;
            electionNotBeforeMs = nowMs() + 1000;
            return;
        }
        List<Map<String, Object>> servers;
        try {
            servers = otherServers();
        } catch (Exception e) {
            System.out.println("[ELEICAO] nao foi possivel listar os servidores no Reference: " + e.getMessage());
            electionDue = true;
            electionNotBeforeMs = nowMs() + 1000;
            return;
        }

        List<Map<String, Object>> higher = new ArrayList<>();
        for (Map<String, Object> server : servers) {
            Object r = server.get("rank");
            if (r instanceof Number && ((Number) r).intValue() > rank) {
                higher.add(server);
            }
        }
        higher.sort(Comparator.comparingInt(s -> ((Number) s.get("rank")).intValue()));
        List<Object> names = new ArrayList<>();
        for (Map<String, Object> server : higher) {
            names.add(server.get("name"));
        }
        System.out.println("[ELEICAO] " + SERVER_NAME + " (rank " + rank + ") inicia eleicao; "
                + "servidores de rank maior: " + names);

        for (Map<String, Object> server : higher) {
            String name = String.valueOf(server.get("name"));
            try {
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("rank", rank);
                Map<String, Object> response = peerRequest(name, "ELECTION_REQUEST", payload);
                Object body = response.get("payload");
                if (body instanceof Map<?, ?> && "OK".equals(((Map<?, ?>) body).get("status"))) {
                    coordinator = null;
                    awaitingAnnouncement = true;
                    awaitingAnnouncementUntilMs = nowMs() + (long) (ELECTION_WAIT_SECONDS * 1000);
                    System.out.println("[ELEICAO] " + name + " respondeu OK; aguardando o anuncio do coordenador");
                    return;
                }
            } catch (Exception e) {
                System.out.println("[ELEICAO] " + name + " nao respondeu (" + e.getMessage() + ")");
            }
        }
        becomeCoordinator();
    }

    @SuppressWarnings("unchecked")
    private static void handleCoordinatorAnnouncement(Map<String, Object> envelope) {
        Object raw = envelope.get("payload");
        if (!(raw instanceof Map<?, ?>)) {
            return;
        }
        Map<String, Object> payload = (Map<String, Object>) raw;
        Object name = payload.get("coordinator");
        Object rank = payload.get("rank");
        Integer mine = myRank();
        if (!(name instanceof String) || SERVER_NAME.equals(name)) {
            return;
        }
        if (rank instanceof Number && mine != null && ((Number) rank).intValue() < mine) {
            // Anuncio de um servidor de rank menor (ex.: antigo coordenador que ainda nao sabe
            // que um servidor maior voltou): este servidor deve assumir.
            if (SERVER_NAME.equals(coordinator)) {
                announceDue = true;
            } else {
                electionDue = true;
            }
            return;
        }
        if (!name.equals(coordinator)) {
            System.out.println("[COORDENADOR] novo coordenador: " + name + " (rank " + rank + ")");
            syncDue = true; // sincroniza logo, sem esperar as proximas mensagens
        }
        coordinator = (String) name;
        awaitingAnnouncement = false;
        electionDue = false;
    }

    /** Le (sem bloquear) o que chegou no topico `servers`. */
    private static void drainAnnouncements() {
        while (true) {
            byte[] topic = subSocket.recv(ZMQ.DONTWAIT);
            if (topic == null) {
                flushDirty(); // grava de uma vez os registros replicados recebidos neste ciclo
                return;
            }
            byte[] body = subSocket.hasReceiveMore() ? subSocket.recv(0) : null;
            if (body == null) {
                continue;
            }
            try {
                String topicName = new String(topic, StandardCharsets.UTF_8);
                @SuppressWarnings("unchecked")
                Map<String, Object> envelope = MAPPER.readValue(body, Map.class);
                observeMessage(envelope);
                // O ZeroMQ casa topicos por prefixo: confere o nome inteiro.
                if (SERVERS_TOPIC.equals(topicName)
                        && "COORDINATOR_ANNOUNCEMENT".equals(envelope.get("type"))) {
                    System.out.println("[PUBSUB-RECV] topic=" + topicName + " " + envelope);
                    handleCoordinatorAnnouncement(envelope);
                } else if (REPLICATION_TOPIC.equals(topicName) && "REPLICATION".equals(envelope.get("type"))) {
                    handleReplication(envelope);
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Falha ao processar mensagem dos topicos de servidores: " + e.getMessage());
            }
        }
    }

    /**
     * Pede a hora ao coordenador e ajusta o relogio (compensando meia latencia). Se o
     * coordenador nao responde, ele e considerado indisponivel e uma eleicao e iniciada.
     */
    private static void syncClock() {
        syncDue = false;
        if (coordinator == null) {
            return; // sem coordenador conhecido; housekeeping() cuida da eleicao
        }
        if (SERVER_NAME.equals(coordinator)) {
            System.out.println("[CLOCK-SYNC] " + SERVER_NAME + " e o coordenador: relogio de referencia "
                    + "(ajuste=" + clockAdjustMs + "ms)");
            return;
        }
        long t0 = localClockMs();
        Map<String, Object> response;
        try {
            response = peerRequest(coordinator, "CLOCK_REQUEST", new LinkedHashMap<>());
        } catch (Exception e) {
            System.out.println("[CLOCK-SYNC] coordenador " + coordinator + " indisponivel ("
                    + e.getMessage() + "); iniciando eleicao");
            coordinator = null;
            electionDue = true;
            return;
        }
        long t1 = localClockMs();
        Object raw = response.get("payload");
        Map<?, ?> payload = raw instanceof Map<?, ?> ? (Map<?, ?>) raw : new LinkedHashMap<>();
        Object coordinatorTime = payload.get("time_ms");
        if (!"OK".equals(payload.get("status")) || !(coordinatorTime instanceof Number)) {
            System.out.println("[CLOCK-SYNC] " + coordinator + " nao e mais o coordenador ("
                    + payload.get("error_msg") + "); iniciando eleicao");
            coordinator = null;
            electionDue = true;
            return;
        }
        // A hora do coordenador foi lida, em media, no meio da troca (estimativa de Cristian).
        long newAdjust = ((Number) coordinatorTime).longValue() + (t1 - t0) / 2 - t1;
        long delta = newAdjust - clockAdjustMs;
        clockAdjustMs = newAdjust;
        System.out.println("[CLOCK-SYNC] coordenador=" + coordinator + " rtt=" + (t1 - t0) + "ms "
                + "correcao=" + (delta >= 0 ? "+" : "") + delta + "ms ajuste_total=" + clockAdjustMs + "ms");
    }

    /** ELECTION_REQUEST de um servidor de rank menor: responde OK e assume a eleicao. */
    private static Map<String, Object> handleElectionRequest(Map<String, Object> envelope) {
        Object raw = envelope.get("payload");
        Object senderRank = raw instanceof Map<?, ?> ? ((Map<?, ?>) raw).get("rank") : null;
        Integer mine = myRank();
        boolean senderIsHigher = senderRank instanceof Number && mine != null
                && ((Number) senderRank).intValue() > mine;
        if (!senderIsHigher) {
            if (SERVER_NAME.equals(coordinator)) {
                announceDue = true;  // ja sou o coordenador: so reafirma
            } else {
                electionDue = true;  // depois de responder, disputa a coordenacao
            }
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", "OK");
        payload.put("rank", serverRank);
        return buildEnvelope("ELECTION_RESPONSE", payload);
    }

    /** CLOCK_REQUEST: so o coordenador responde com a hora correta. */
    private static Map<String, Object> handleClockRequest() {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (!SERVER_NAME.equals(coordinator)) {
            payload.put("status", "ERROR");
            payload.put("error_msg", "este servidor nao e o coordenador");
            payload.put("coordinator", coordinator);
            return buildEnvelope("CLOCK_RESPONSE", payload);
        }
        payload.put("status", "OK");
        payload.put("time_ms", physicalNowMs());
        return buildEnvelope("CLOCK_RESPONSE", payload);
    }

    /**
     * Tarefas periodicas do loop principal: registro no Reference, eleicao, reanuncio do
     * coordenador e sincronizacao de relogio.
     */
    private static void housekeeping() {
        maybeRegister();
        if (!referenceRegistered) {
            return;
        }
        long now = nowMs();
        if (coordinator == null) {
            if (!awaitingAnnouncement) {
                electionDue = true; // ninguem conhecido e nenhuma eleicao em andamento
            } else if (now >= awaitingAnnouncementUntilMs) {
                System.out.println("[ELEICAO] o anuncio do coordenador nao chegou a tempo; refazendo a eleicao");
                awaitingAnnouncement = false;
                electionDue = true;
            }
        }
        if (electionDue && now >= electionNotBeforeMs) {
            runElection();
        }
        if (SERVER_NAME.equals(coordinator) && (announceDue || nowMs() >= nextAnnouncementAtMs)) {
            announceCoordinator();
        }
        if (syncDue) {
            syncClock();
            if (electionDue && nowMs() >= electionNotBeforeMs) {
                runElection(); // o coordenador caiu durante a sincronizacao
            }
        }
        replicationHousekeeping();
    }

    /** Chamado depois de responder uma mensagem de cliente (nao atrasa a resposta). */
    private static void afterClientMessage() {
        // Usa a contagem acumulada: mensagens atendidas durante uma espera tambem contam.
        if (clientMessagesReceived >= nextHeartbeatAt) {
            nextHeartbeatAt = (clientMessagesReceived / HEARTBEAT_EVERY + 1) * HEARTBEAT_EVERY;
            sendHeartbeat();
        }
        if (clientMessagesReceived >= nextSyncCount) {
            nextSyncCount = (clientMessagesReceived / SYNC_EVERY + 1) * SYNC_EVERY;
            syncDue = true;
        }
    }

    // ------------------------------------------------------------------
    // Parte 5: replicacao ativa com anti-entropia
    //
    // Todo servidor guarda TODOS os registros (logins, canais e mensagens). Cada registro
    // recebe um id (origin, seq): `origin` e o servidor que o gravou e `seq` um contador
    // por origem (compartilhado pelos tres tipos). Logo apos gravar, o servidor publica o
    // registro no topico `replication`; os demais o aplicam de forma idempotente (um id ja
    // visto e ignorado). O Pub/Sub nao garante entrega, entao ha anti-entropia: cada
    // servidor conhece, por origem, o maior seq sem buracos (`replContiguous`) e pede aos
    // pares (SYNC_REQUEST) o que falta -- ao iniciar, ao detectar um buraco e periodicamente.
    // ------------------------------------------------------------------

    private static List<Map<String, Object>> kindList(String kind) {
        switch (kind) {
            case "login":
                return knownLogins;
            case "channel":
                return knownChannels;
            default:
                return knownMessages;
        }
    }

    private static String kindPath(String kind) {
        switch (kind) {
            case "login":
                return LOGINS_PATH;
            case "channel":
                return CHANNELS_PATH;
            default:
                return MESSAGES_PATH;
        }
    }

    private static void saveKind(String kind) {
        Persistence.saveList(kindList(kind), kindPath(kind));
    }

    private static void flushDirty() {
        for (String kind : new ArrayList<>(dirtyKinds)) {
            saveKind(kind);
        }
        dirtyKinds.clear();
    }

    private static long numberOr(Object value, long fallback) {
        return value instanceof Number ? ((Number) value).longValue() : fallback;
    }

    /** Ordem total e igual em todos os servidores: relogio logico, origem e seq. */
    private static int compareRecords(Map<String, Object> a, Map<String, Object> b) {
        int c = Long.compare(numberOr(a.get("lc"), 0), numberOr(b.get("lc"), 0));
        if (c != 0) {
            return c;
        }
        c = String.valueOf(a.get("origin")).compareTo(String.valueOf(b.get("origin")));
        if (c != 0) {
            return c;
        }
        return Long.compare(numberOr(a.get("seq"), 0), numberOr(b.get("seq"), 0));
    }

    private static void markSeen(String origin, long seq) {
        Set<Long> seen = replSeen.computeIfAbsent(origin, k -> new HashSet<>());
        seen.add(seq);
        long n = replContiguous.getOrDefault(origin, 0L);
        while (seen.contains(n + 1)) {
            n++;
        }
        replContiguous.put(origin, n);
        if (origin.equals(SERVER_NAME) && seq >= nextSeq) {
            nextSeq = seq + 1;
        }
    }

    private static boolean validRecord(Object kind, Object record) {
        if (!(kind instanceof String) || !KINDS.contains(kind) || !(record instanceof Map<?, ?>)) {
            return false;
        }
        Map<?, ?> r = (Map<?, ?>) record;
        Object origin = r.get("origin");
        Object seq = r.get("seq");
        return origin instanceof String && !((String) origin).isEmpty()
                && seq instanceof Number && ((Number) seq).longValue() > 0;
    }

    /** Aplica um registro (local ou replicado). Idempotente: retorna false se o id ja foi visto. */
    private static boolean applyRecord(String kind, Map<String, Object> record) {
        String origin = (String) record.get("origin");
        long seq = ((Number) record.get("seq")).longValue();
        Set<Long> seen = replSeen.get(origin);
        if (seen != null && seen.contains(seq)) {
            return false;
        }
        markSeen(origin, seq);
        kindList(kind).add(record);
        if ("channel".equals(kind) && record.get("channel_name") instanceof String) {
            String name = (String) record.get("channel_name");
            Map<String, Object> current = channelWinners.get(name);
            // Criacoes concorrentes do mesmo canal em servidores diferentes: vence a de menor
            // (relogio logico, origem, seq) -- o mesmo criterio em todos os servidores.
            if (current == null || compareRecords(record, current) < 0) {
                channelWinners.put(name, record);
            }
        }
        return true;
    }

    /** Carrega os dados do disco e reconstroi os indices de replicacao. */
    @SuppressWarnings("unchecked")
    private static void loadState() {
        knownLogins = Persistence.loadList(LOGINS_PATH);
        knownChannels = Persistence.loadList(CHANNELS_PATH);
        knownMessages = Persistence.loadList(MESSAGES_PATH);
        List<Object[]> legacy = new ArrayList<>();
        for (String kind : KINDS) {
            for (Map<String, Object> record : kindList(kind)) {
                if (validRecord(kind, record)) {
                    markSeen((String) record.get("origin"), ((Number) record.get("seq")).longValue());
                } else {
                    legacy.add(new Object[] {kind, record});
                }
            }
        }
        // Registros gravados antes da Parte 5 (sem id): migra como se fossem deste servidor.
        Set<String> migrated = new LinkedHashSet<>();
        for (Object[] item : legacy) {
            Map<String, Object> record = (Map<String, Object>) item[1];
            long seq = nextSeq;
            record.put("origin", SERVER_NAME);
            record.put("seq", seq);
            record.putIfAbsent("lc", 0L);
            markSeen(SERVER_NAME, seq);
            migrated.add((String) item[0]);
        }
        for (Map<String, Object> record : knownChannels) {
            if (record.get("channel_name") instanceof String) {
                String name = (String) record.get("channel_name");
                Map<String, Object> current = channelWinners.get(name);
                if (current == null || compareRecords(record, current) < 0) {
                    channelWinners.put(name, record);
                }
            }
        }
        for (String kind : migrated) {
            saveKind(kind);
        }
        if (!legacy.isEmpty()) {
            System.out.println("[REPLICA] " + legacy.size()
                    + " registro(s) antigo(s) migrado(s) para o formato com id");
        }
    }

    private static void replicate(String kind, Map<String, Object> record) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("kind", kind);
        payload.put("record", record);
        Map<String, Object> envelope = buildEnvelope("REPLICATION", payload);
        System.out.println("[REPLICA-SEND] topic=" + REPLICATION_TOPIC + " " + envelope);
        try {
            byte[] body = MAPPER.writeValueAsBytes(envelope);
            pubSocket.sendMore(REPLICATION_TOPIC.getBytes(StandardCharsets.UTF_8));
            pubSocket.send(body, 0);
        } catch (Exception e) {
            // O registro ja esta em disco: a anti-entropia entrega aos demais depois.
            System.err.println("[ERROR] Falha ao replicar: " + e.getMessage());
        }
    }

    /** Grava um registro originado neste servidor: id, disco e so entao replicacao. */
    private static void localStore(String kind, Map<String, Object> record) {
        record.put("origin", SERVER_NAME);
        record.put("seq", nextSeq);
        record.put("lc", logicalClock);
        applyRecord(kind, record);
        saveKind(kind);
        replicate(kind, record);
    }

    @SuppressWarnings("unchecked")
    private static void handleReplication(Map<String, Object> envelope) {
        Object raw = envelope.get("payload");
        Map<String, Object> payload = raw instanceof Map<?, ?> ? (Map<String, Object>) raw : new LinkedHashMap<>();
        Object kind = payload.get("kind");
        Object record = payload.get("record");
        if (!validRecord(kind, record)) {
            System.out.println("[ERROR] REPLICATION invalida ignorada: " + envelope);
            return;
        }
        Map<String, Object> rec = (Map<String, Object>) record;
        String origin = (String) rec.get("origin");
        if (origin.equals(SERVER_NAME)) {
            return; // eco do que este servidor mesmo publicou
        }
        boolean applied = applyRecord((String) kind, rec);
        System.out.println("[REPLICA-RECV] " + (applied ? "aplicado" : "duplicado") + " " + envelope);
        if (applied) {
            dirtyKinds.add((String) kind);
            if (numberOr(rec.get("seq"), 0) > replContiguous.getOrDefault(origin, 0L)) {
                replGapPeers.add(origin); // chegou fora de ordem: ha buraco
            }
        }
    }

    /** Aplica um lote do SYNC_RESPONSE (inclui registros deste proprio servidor que ele havia perdido). */
    @SuppressWarnings("unchecked")
    private static int applyBatch(List<?> items) {
        int applied = 0;
        for (Object item : items) {
            if (!(item instanceof Map<?, ?>)) {
                continue;
            }
            Object kind = ((Map<?, ?>) item).get("kind");
            Object record = ((Map<?, ?>) item).get("record");
            if (validRecord(kind, record) && applyRecord((String) kind, (Map<String, Object>) record)) {
                dirtyKinds.add((String) kind);
                applied++;
            }
        }
        return applied;
    }

    /** SYNC_REQUEST de um par: devolve os registros acima do que ele ja tem (`have`). */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> handleSyncRequest(Map<String, Object> envelope) {
        Object raw = envelope.get("payload");
        Map<String, Object> payload = raw instanceof Map<?, ?> ? (Map<String, Object>) raw : new LinkedHashMap<>();
        Object haveRaw = payload.get("have");
        Map<?, ?> have = haveRaw instanceof Map<?, ?> ? (Map<?, ?>) haveRaw : new LinkedHashMap<>();
        long limit = REPL_BATCH;
        Object requested = payload.get("limit");
        if (requested instanceof Number && ((Number) requested).longValue() >= 1) {
            limit = Math.min(((Number) requested).longValue(), REPL_BATCH);
        }
        List<Object[]> missing = new ArrayList<>(); // {seq, origem, tipo, registro}
        for (String kind : KINDS) {
            for (Map<String, Object> record : kindList(kind)) {
                Object known = have.get(record.get("origin"));
                long knownSeq = known instanceof Number ? ((Number) known).longValue() : 0;
                long seq = numberOr(record.get("seq"), 0);
                if (seq > knownSeq) {
                    missing.add(new Object[] {seq, String.valueOf(record.get("origin")), kind, record});
                }
            }
        }
        // Por seq: cada origem sai em ordem crescente.
        missing.sort((x, y) -> {
            int c = Long.compare((Long) x[0], (Long) y[0]);
            return c != 0 ? c : ((String) x[1]).compareTo((String) y[1]);
        });
        int size = (int) Math.min(limit, missing.size());
        List<Object> records = new ArrayList<>();
        for (Object[] item : missing.subList(0, size)) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("kind", item[2]);
            entry.put("record", item[3]);
            records.add(entry);
        }
        System.out.println("[SYNC] pedido de " + envelope.get("sender_id") + ": " + missing.size()
                + " registro(s) acima do que ele tem; enviando " + size);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", "OK");
        out.put("records", records);
        out.put("more", missing.size() > size);
        return buildEnvelope("SYNC_RESPONSE", out);
    }

    /** Anti-entropia: puxa de um par os registros que faltam. Retorna quantos foram aplicados. */
    private static int syncWithPeer(String name) throws Exception {
        int total = 0;
        for (int i = 0; i < REPL_MAX_BATCHES; i++) {
            Map<String, Object> have = new LinkedHashMap<>(replContiguous);
            System.out.println("[PEER-SEND] para=" + name + " SYNC_REQUEST have=" + have + " limit=" + REPL_BATCH);
            Map<String, Object> request = new LinkedHashMap<>();
            request.put("have", have);
            request.put("limit", REPL_BATCH);
            Map<String, Object> response = peerRequest(name, "SYNC_REQUEST", request, false);
            Object raw = response.get("payload");
            Map<?, ?> body = raw instanceof Map<?, ?> ? (Map<?, ?>) raw : new LinkedHashMap<>();
            if (!"OK".equals(body.get("status"))) {
                throw new IllegalStateException(String.valueOf(body.get("error_msg")));
            }
            List<?> records = body.get("records") instanceof List<?> ? (List<?>) body.get("records") : new ArrayList<>();
            int applied = applyBatch(records);
            total += applied;
            boolean more = Boolean.TRUE.equals(body.get("more"));
            System.out.println("[PEER-RECV] de=" + name + " SYNC_RESPONSE registros=" + records.size()
                    + " novos=" + applied + " mais=" + more);
            if (!more || applied == 0) {
                break;
            }
        }
        flushDirty();
        return total;
    }

    private static void pullFrom(String name) {
        try {
            int total = syncWithPeer(name);
            System.out.println("[SYNC] sincronizado com " + name + ": " + total + " registro(s) novo(s)");
        } catch (Exception e) {
            System.out.println("[SYNC] nao foi possivel sincronizar com " + name + " (" + e.getMessage() + ")");
        }
    }

    private static void scheduleNextSync() {
        nextReplSyncAtMs = nowMs() + (long) (REPL_SYNC_SECONDS * 1000 * (0.7 + RANDOM.nextDouble() * 0.6));
    }

    /**
     * Anti-entropia: na partida, ao detectar buraco e periodicamente (com variacao aleatoria,
     * para dois servidores nao se pedirem dados ao mesmo tempo).
     */
    private static void replicationHousekeeping() {
        if (!referenceRegistered) {
            return;
        }
        long now = nowMs();
        if (now < replNextAttemptMs) {
            return;
        }
        if (!replInitialSyncDone) {
            List<Map<String, Object>> peers;
            try {
                peers = otherServers();
            } catch (Exception e) {
                System.out.println("[SYNC] nao foi possivel listar os servidores no Reference: " + e.getMessage());
                replNextAttemptMs = now + 1000;
                return;
            }
            for (Map<String, Object> peer : peers) {
                pullFrom(String.valueOf(peer.get("name")));
            }
            replInitialSyncDone = true;
            scheduleNextSync();
            return;
        }
        if (!replGapPeers.isEmpty()) {
            for (String name : new ArrayList<>(replGapPeers)) {
                replGapPeers.remove(name);
                pullFrom(name);
            }
        }
        if (now >= nextReplSyncAtMs) {
            scheduleNextSync();
            List<Map<String, Object>> peers;
            try {
                peers = otherServers();
            } catch (Exception e) {
                return;
            }
            if (!peers.isEmpty()) {
                Map<String, Object> peer = peers.get(replPeerCursor % peers.size());
                replPeerCursor++;
                pullFrom(String.valueOf(peer.get("name")));
            }
        }
    }

    /** HISTORY_REQUEST: ultimas mensagens de um canal, em ordem (relogio logico, origem, seq). */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> handleHistoryRequest(Map<String, Object> envelope) {
        Object raw = envelope.get("payload");
        Map<String, Object> payload = raw instanceof Map<?, ?> ? (Map<String, Object>) raw : new LinkedHashMap<>();
        String error = validateRequiredString(payload, "channel_name");
        if (error != null) {
            return errorResponse("HISTORY_RESPONSE", error);
        }
        String channelName = (String) payload.get("channel_name");
        if (!channelWinners.containsKey(channelName)) {
            return errorResponse("HISTORY_RESPONSE", "canal '" + channelName + "' nao existe");
        }
        long limit = 50;
        Object requested = payload.get("limit");
        if (requested instanceof Number && ((Number) requested).longValue() >= 1) {
            limit = ((Number) requested).longValue();
        }
        limit = Math.min(limit, 500);
        List<Map<String, Object>> messages = new ArrayList<>();
        for (Map<String, Object> record : knownMessages) {
            if (channelName.equals(record.get("channel_name"))) {
                messages.add(record);
            }
        }
        messages.sort(Server::compareRecords);
        int from = (int) Math.max(0, messages.size() - limit);
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> record : messages.subList(from, messages.size())) {
            Map<String, Object> entry = new LinkedHashMap<>();
            for (String key : new String[] {"origin", "seq", "sender_id", "sender_lang", "message", "timestamp"}) {
                entry.put(key, record.get(key));
            }
            out.add(entry);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "OK");
        result.put("channel_name", channelName);
        result.put("total", messages.size());
        result.put("messages", out);
        return buildEnvelope("HISTORY_RESPONSE", result);
    }

    /**
     * Decodifica, atende e RESPONDE uma requisicao do REP principal. Retorna true se era uma
     * troca entre servidores (nao conta como mensagem de cliente).
     */
    private static boolean processRaw(byte[] raw) throws Exception {
        // Cada mensagem e tratada isoladamente: qualquer falha ao decodificar o MessagePack,
        // ou ao processar o envelope (campos ausentes, tipos inesperados, envelope que nao e
        // um Map, etc.), vira um ERROR_RESPONSE, em vez de encerrar o servidor. O socket REP
        // exige exatamente 1 send() por recv(), entao SEMPRE respondemos algo.
        Map<String, Object> response;
        boolean peerMessage = false;
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> envelope = MAPPER.readValue(raw, Map.class);
            observeMessage(envelope);
            System.out.println("[RECV] " + envelope);
            peerMessage = isPeerType(envelope.get("type"));
            response = dispatch(envelope);
        } catch (Exception e) {
            System.err.println("[ERROR] Falha ao processar mensagem: " + e.getMessage());
            response = errorResponse(
                    "ERROR_RESPONSE",
                    "mensagem invalida ou mal-formada: " + e.getMessage());
        }
        byte[] out = MAPPER.writeValueAsBytes(response);
        repSocket.send(out, 0);
        System.out.println("[SEND] " + response);
        if (!peerMessage) {
            clientMessagesReceived++;
        }
        return peerMessage;
    }

    /**
     * Espera a resposta de um socket REQ SEM deixar de atender o REP principal (retorna null
     * no timeout). Dois servidores que se pedem algo ao mesmo tempo (ex.: anti-entropia ou
     * relogio) ficariam cada um bloqueado esperando o outro ate o timeout. Enquanto espera, o
     * servidor atende as requisicoes que chegam no REP (so responde; heartbeat e demais tarefas
     * ficam para o loop principal). E seguro porque todo pedido a pares/Reference parte de um
     * ponto em que o REP ja respondeu a ultima requisicao.
     */
    private static byte[] recvWhileServing(ZMQ.Socket sock, int timeoutMs) throws Exception {
        long deadline = nowMs() + timeoutMs;
        while (true) {
            byte[] raw = sock.recv(ZMQ.DONTWAIT);
            if (raw != null) {
                return raw;
            }
            byte[] incoming = repSocket.recv(ZMQ.DONTWAIT);
            if (incoming != null) {
                processRaw(incoming);
                continue;
            }
            if (nowMs() >= deadline) {
                return null;
            }
            Thread.sleep(2);
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
        if ("HISTORY_REQUEST".equals(type)) {
            return handleHistoryRequest(envelope);
        }

        // Parte 4: trocas entre servidores.
        if ("ELECTION_REQUEST".equals(type)) {
            return handleElectionRequest(envelope);
        }
        if ("CLOCK_REQUEST".equals(type)) {
            return handleClockRequest();
        }
        // Parte 5: anti-entropia entre servidores.
        if ("SYNC_REQUEST".equals(type)) {
            return handleSyncRequest(envelope);
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

        localStore("login", record);

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

        // `servers` (Parte 4) e `replication` (Parte 5) sao topicos internos dos servidores.
        if (SERVERS_TOPIC.equals(channelName) || REPLICATION_TOPIC.equals(channelName)) {
            return errorResponse("CHANNEL_CREATE_RESPONSE",
                    "nome de canal reservado: '" + channelName + "'");
        }

        // Agora vale para o canal em QUALQUER servidor, ja que todos replicam todos os canais.
        if (channelWinners.containsKey(channelName)) {
            return errorResponse("CHANNEL_CREATE_RESPONSE", "canal '" + channelName + "' ja existe");
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("channel_name", channelName);
        record.put("created_by", botName);
        record.put("timestamp", envelope.get("timestamp"));

        localStore("channel", record);

        System.out.printf(
                "[CHANNEL_CREATE] channel='%s' created_by='%s' (total de canais: %d)%n",
                channelName, botName, channelWinners.size());

        return okResponse("CHANNEL_CREATE_RESPONSE");
    }

    private static Map<String, Object> handleChannelListRequest() {
        List<Map<String, Object>> winners = new ArrayList<>(channelWinners.values());
        winners.sort(Server::compareRecords);
        List<Object> channelNames = new ArrayList<>();
        for (Map<String, Object> c : winners) {
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

        if (!channelWinners.containsKey(channelName)) {
            return errorResponse("PUBLISH_RESPONSE", "canal '" + channelName + "' nao existe");
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("channel_name", channelName);
        record.put("sender_id", envelope.get("sender_id"));
        record.put("sender_lang", envelope.get("sender_lang"));
        record.put("message", message);
        record.put("timestamp", envelope.get("timestamp"));

        // 1) Grava em disco e replica para os demais servidores (durabilidade
        // antes de visibilidade).
        localStore("message", record);

        // 2) So entao publica no broker, para subscribers ao vivo.
        Map<String, Object> publishPayload = new LinkedHashMap<>();
        publishPayload.put("channel_name", channelName);
        publishPayload.put("message", message);
        Map<String, Object> publishEnvelope = buildEnvelope("CHANNEL_MESSAGE", publishPayload);

        try {
            byte[] topic = channelName.getBytes("UTF-8");
            byte[] body = MAPPER.writeValueAsBytes(publishEnvelope);
            System.out.println("[PUB-SEND] topic=" + channelName + " " + publishEnvelope);
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
