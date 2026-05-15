package com.milestone.basilisk.vertx;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("unused")
public class BusClient {
    private static final long CONNECT_RETRY_BASE_MS = 500;
    private static final long CONNECT_RETRY_MAX_MS = 30_000;
    private static final long CONNECT_RETRY_MAX_JITTER_MS = 500;
    private static final int CONNECT_RETRY_MAX_ATTEMPTS = 8;

    private final Vertx vertx;
    private final String serviceId, instanceId;
    private final String connectionKey;
    private final NetSocket socket;
    private final ArrayDeque<Promise<JsonObject>> pending = new ArrayDeque<>();
    private final Map<String, List<Consumer<JsonObject>>> eventHandlers = new ConcurrentHashMap<>();
    private final Map<String, BiFunction<JsonObject, RequestResponder, Future<Void>>> requestHandlers = new ConcurrentHashMap<>();
    private String buffer = "";
    private static final Logger log = LoggerFactory.getLogger(BusClient.class);

    private BusClient(Vertx vertx, String serviceId, String instanceId, NetSocket socket) {
        this.vertx = vertx;
        this.serviceId = serviceId;
        this.instanceId = instanceId;
        this.connectionKey = serviceId + ":" + instanceId;
        this.socket = socket;
        log.info(("bus client connected connection_key=%s remote=%s").formatted(connectionKey, socket.remoteAddress()));
        socket.closeHandler(v -> log.warn(("bus client socket closed connection_key=%s remote=%s").formatted(connectionKey, socket.remoteAddress())));
        socket.exceptionHandler(err -> log.error(("bus client socket error connection_key=%s remote=%s cause=%s").formatted(connectionKey, socket.remoteAddress(), err.getMessage()), err));
        bindReadLoop();
    }

    private static long computeRetryDelayMs(int attempt) {
        int exp = Math.min(attempt, 16);
        long base = CONNECT_RETRY_BASE_MS << exp;
        if (base > CONNECT_RETRY_MAX_MS) {
            base = CONNECT_RETRY_MAX_MS;
        }
        long jitter = ThreadLocalRandom.current().nextLong(CONNECT_RETRY_MAX_JITTER_MS + 1);
        return base + jitter;
    }


    public static Future<BusClient> connect(Vertx vertx, String host, int port, String serviceId, String instanceId, String token) {
        return connectWithRetry(vertx, host, port, serviceId, instanceId, token, 0);
    }

    private static Future<BusClient> connectWithRetry(Vertx vertx, String host, int port, String serviceId, String instanceId, String token, int attempt) {
        if (attempt >= CONNECT_RETRY_MAX_ATTEMPTS) {
            return Future.failedFuture("Failed to connect to service bus after retry attempts");
        }

        final var netClient = vertx.createNetClient(
            new NetClientOptions()
                .setReconnectAttempts(512)
                .setReconnectInterval(5000)
                .setTcpKeepAlive(true)
                .setTcpNoDelay(true)
                .setIdleTimeout(0)
                .setReadIdleTimeout(0)
        );

        log.info(("bus client connect attempt=%d connection_key=%s target=%s:%d").formatted(attempt + 1, serviceId + ":" + instanceId, host, port));
        return netClient.connect(port, host)
            .compose(socket -> {
                var client = new BusClient(vertx, serviceId, instanceId, socket);
                return client.sendCommand(new JsonObject()
                    .put("type", ProtocolTypes.CONNECT)
                    .put("serviceId", serviceId)
                    .put("instanceId", instanceId)
                    .put("token", token))
                .andThen(v -> {
                    if(v.failed()) {
                        log.warn(("bus client connect handshake failed connection_key=%s cause=%s").formatted(client.connectionKey, v.cause() == null ? "unknown" : v.cause().getMessage()));
                        return;
                    }

                    log.info(("bus client authenticated connection_key=%s remote=%s").formatted(client.connectionKey, client.socket.remoteAddress()));

                    vertx.setPeriodic(20_000, id -> client.publish(
                      "basilisk.metrics.distribution",
                "basilisk.internal",
                new JsonObject().put("name", "memory_usage").put("value", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).put("unit", "bytes")
                    ).andThen(res -> {
                        if(res.failed()) {
                            log.warn(("bus client metric publish failed connection_key=%s cause=%s").formatted(client.connectionKey, res.cause() == null ? "unknown" : res.cause().getMessage()));
                            return;
                        }

                        log.info(("bus client metric published connection_key=%s subscribers=%s").formatted(client.connectionKey, res.result()));
                    }));
                })
                .map(client);
            })
            .recover(err -> {
                log.warn(("bus client connect failed attempt=%d connection_key=%s cause=%s").formatted(attempt + 1, serviceId + ":" + instanceId, err.getMessage()));
                long delayMs = computeRetryDelayMs(attempt);
                log.info(("bus client retry scheduled connection_key=%s delay_ms=%d").formatted(serviceId + ":" + instanceId, delayMs));
                return vertx.timer(delayMs)
                        .compose(ignored -> connectWithRetry(vertx, host, port, serviceId, instanceId, token, attempt + 1));
            });
    }

    public Future<Void> subscribe(List<String> topics) {
        return sendCommand(new JsonObject().put("type", ProtocolTypes.SUBSCRIBE).put("topics", topics)).mapEmpty();
    }

    public Future<Void> unsubscribe(List<String> topics) {
        return sendFireAndForget(new JsonObject().put("type", ProtocolTypes.UNSUBSCRIBE).put("topics", topics));
    }

    public Future<Integer> publish(String topic, String messageType, JsonObject payload) {
        var event = new JsonObject()
            .put("eventId", UUID.randomUUID().toString())
            .put("emittedAtUtc", Instant.now().toString())
            .put("serviceId", this.serviceId)
            .put("instanceId", this.instanceId)
            .put("topic", topic)
            .put("messageType", messageType)
            .put("correlationId", 0)
            .put("payload", payload);
        return publishEvent(event);
    }

    public Future<Integer> publishEvent(JsonObject event) {
        return sendCommand(new JsonObject().put("type", ProtocolTypes.PUBLISH).put("event", event))
                .map(message -> message.getInteger("subscriberCount", 0));
    }

    public Future<JsonObject> forward(ForwardRequest request) {
        var msg = new JsonObject()
                .put("type", ProtocolTypes.FORWARD)
                .put("forwardRequest", new JsonObject()
                        .put("targetServiceId", request.targetServiceId)
                        .put("messageType", request.messageType)
                        .put("payload", request.payload)
                        .put("timeoutMs", request.timeoutMs));
        return sendCommandExpect(msg).compose(resp -> {
            if (!ProtocolTypes.FORWARD_RESPONSE.equals(resp.getString("type"))) {
                return Future.failedFuture("unexpected protocol message type: " + resp.getString("type"));
            }
            return Future.succeededFuture(resp.getJsonObject("forwardResponse"));
        });
    }

    public Future<Void> onEvent(String topic, Consumer<JsonObject> handler) {
        return subscribe(List.of(topic)).onSuccess(ignored -> eventHandlers.computeIfAbsent(topic, k -> new ArrayList<>()).add(handler));
    }

    public Future<Void> onRequest(String topic, BiFunction<JsonObject, RequestResponder, Future<Void>> handler) {
        return subscribe(List.of("service-" + serviceId)).onSuccess(ignored -> requestHandlers.put(topic, handler));
    }

    public Future<Void> close() {
        var promise = Promise.<Void>promise();
        socket.close();
        return promise.future();
    }

    private Future<JsonObject> sendCommand(JsonObject message) {
        return sendCommandExpect(message).compose(response -> {
            if (!ProtocolTypes.ACK.equals(response.getString("type"))) {
                return Future.failedFuture("unexpected protocol message type: " + response.getString("type"));
            }
            return Future.succeededFuture(response);
        });
    }

    private Future<JsonObject> sendCommandExpect(JsonObject message) {
        var promise = Promise.<JsonObject>promise();
        pending.add(promise);
        socket.write(message.encode() + "\n");
        vertx.setTimer(10_000, ignored -> {
            if (!promise.future().isComplete()) {
                promise.fail("Timed out waiting for protocol response");
            }
        });
        return promise.future().compose(resp -> {
            if (ProtocolTypes.ERROR.equals(resp.getString("type"))) {
                return Future.failedFuture("protocol error " + resp.getString("errorCode", "UNKNOWN_ERROR") + ": " + resp.getString("message", "Service bus protocol error"));
            }
            return Future.succeededFuture(resp);
        });
    }

    private Future<Void> sendFireAndForget(JsonObject message) {
        socket.write(message.encode() + "\n");
        return Future.succeededFuture();
    }

    private void bindReadLoop() {
        socket.handler(bufferChunk -> {
            buffer += bufferChunk.toString();
            int index = buffer.indexOf('\n');
            while (index >= 0) {
                var line = buffer.substring(0, index);
                buffer = buffer.substring(index + 1);
                index = buffer.indexOf('\n');
                JsonObject message;
                try {
                    message = new JsonObject(line);
                } catch (Exception ignored) {
                    continue;
                }

                var type = message.getString("type");
                if (ProtocolTypes.EVENT.equals(type)) {
                    var event = message.getJsonObject("event");
                    if (event != null) {
                        dispatchEvent(event);
                    }
                    continue;
                }

                if (ProtocolTypes.ACK.equals(type) || ProtocolTypes.ERROR.equals(type) || ProtocolTypes.FORWARD_RESPONSE.equals(type)) {
                    var next = pending.poll();
                    if (next != null) {
                        next.complete(message);
                    }
                }
            }
        });
    }

    private void dispatchEvent(JsonObject event) {
        var topic = event.getString("topic");
        eventHandlers.getOrDefault(topic, List.of()).forEach(handler -> vertx.runOnContext(v -> handler.accept(event)));
        eventHandlers.getOrDefault("*", List.of()).forEach(handler -> vertx.runOnContext(v -> handler.accept(event)));

        if (("service-" + serviceId).equals(topic)) {
            var messageType = event.getString("messageType");
            var handler = requestHandlers.get(messageType);
            var payload = event.getJsonObject("payload", new JsonObject());
            var replyTo = payload.getString("reply_to");
            if (handler != null && replyTo != null) {
                var responder = new RequestResponder(replyTo, event.getString("eventId"), event.getLong("correlationId", 0L), messageType);
                handler.apply(event, responder);
            }
        }
    }

    public record ForwardRequest(String targetServiceId, String messageType,
                                 JsonObject payload, Long timeoutMs) {
    }

    public class RequestResponder {
        private final String replyTo;
        private final String causationId;
        private final long correlationId;
        private final String defaultMessageType;

        RequestResponder(String replyTo, String causationId, long correlationId, String defaultMessageType) {
            this.replyTo = replyTo;
            this.causationId = causationId;
            this.correlationId = correlationId;
            this.defaultMessageType = defaultMessageType;
        }

        public Future<Integer> respond(String messageType, JsonObject payload) {
            var event = new JsonObject()
                    .put("eventId", "")
                    .put("emittedAtUtc", Instant.now().toString())
                    .put("serviceId", "")
                    .put("instanceId", "")
                    .put("topic", replyTo)
                    .put("messageType", messageType)
                    .put("correlationId", correlationId)
                    .put("causationId", causationId)
                    .put("payload", payload);
            return publishEvent(event);
        }

        public Future<Integer> respondOk(JsonObject payload) {
            return respond(defaultMessageType, payload);
        }
    }
}
