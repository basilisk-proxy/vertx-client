package com.milestone.basilisk.vertx;

import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Top-level Basilisk client that composes gateway registration APIs and service bus APIs.
 */
@SuppressWarnings("unused")
public class BasiliskClient {
    /** Gateway registry API client. */
    public final GatewayApiClient gateway;

    /** Authenticated service bus client. */
    public final BusClient bus;

    /** Service identifier used during registration/connect. */
    public final String serviceId;

    /** Gateway-issued instance identifier for this client session. */
    public final String instanceId;

    private BasiliskClient(GatewayApiClient gateway, BusClient bus, String serviceId, String instanceId) {
        this.gateway = gateway;
        this.bus = bus;
        this.serviceId = serviceId;
        this.instanceId = instanceId;
    }

    /**
     * Connects using registration-first semantics.
     *
     * <p>The flow is: register an instance on the gateway with empty instance ID, read generated
     * instance credentials, then authenticate the bus connection using returned identity/token.</p>
     */
    public static Future<BasiliskClient> connect(Vertx vertx, BasiliskClientConfig config) {
        var gateway = new GatewayApiClient(vertx, config.gatewayBaseUrl());
        var prefixes = new JsonArray();
        config.pathPrefixes().forEach(prefixes::add);
        var request = new JsonObject()
                .put("serviceId", config.serviceId())
                .put("fingerprint", config.fingerprint())
                .put("pathPrefixes", prefixes)
                .put("instance", new JsonObject()
                        .put("instanceId", "")
                        .put("scheme", config.scheme())
                        .put("host", config.host())
                        .put("port", config.port())
                        .put("weight", config.weight()))
                .put("auth", new JsonObject()
                        .put("type", config.registrationAuthType())
                        .put("token", config.registrationToken()));

        return gateway.registerInstanceAuto(request)
                .compose(reg -> BusClient.connect(vertx, config.busHost(), config.busPort(),
                                config.serviceId(), reg.getString("instanceId"), reg.getString("token"))
                        .map(bus -> new BasiliskClient(gateway, bus, config.serviceId(), reg.getString("instanceId"))));
    }

    /**
     * Deregisters this instance from the gateway service registry.
     */
    public Future<Void> deregister() {
        return gateway.deregisterInstance(serviceId, instanceId);
    }

    /**
     * Immutable connect-time configuration for registration and bus authentication.
     */
    public record BasiliskClientConfig(
            String gatewayBaseUrl,
            String busHost,
            int busPort,
            String serviceId,
            String fingerprint,
            List<String> pathPrefixes,
            String scheme,
            String host,
            int port,
            int weight,
            String registrationAuthType,
            String registrationToken
    ) {
    }
}
