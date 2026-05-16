package com.milestone.basilisk.vertx;

import java.util.Objects;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientAgent;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonObject;

/**
 * HTTP client for Basilisk gateway registry APIs.
 */
@SuppressWarnings("unused")
public class GatewayApiClient {
    private final String baseUrl;
    private final HttpClientAgent webClient;

    public GatewayApiClient(Vertx vertx, String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.webClient = vertx.createHttpClient();
    }

    /**
     * Registers an instance while forcing generated identity mode.
     *
     * <p>This method sets {@code instance.instanceId = ""} and delegates to
     * {@link #registerInstance(JsonObject)}.</p>
     *
     * @param request registry registration payload
     * @return gateway JSON response containing issued instance credentials
     */
    public Future<JsonObject> registerInstanceAuto(JsonObject request) {
        var copy = Objects.requireNonNullElse(request.copy(), new JsonObject());
        copy.getJsonObject("instance").put("instanceId", "");
        return registerInstance(copy);
    }

    /**
     * Registers an instance in the gateway registry.
     *
     * @param request registration payload
     * @return gateway JSON response body
     */
    public Future<JsonObject> registerInstance(JsonObject request) {
        return webClient.request(new RequestOptions().setMethod(HttpMethod.POST).setAbsoluteURI(baseUrl + "/registry/register").setHeaders(MultiMap.caseInsensitiveMultiMap().add("Content-Type", "application/json")))
                .compose(httpClientRequest -> httpClientRequest.send(request.encode()))
                .compose(resp -> resp.body().map(Buffer::toJsonObject));
    }

    /**
     * Deregisters one service instance from the gateway registry.
     *
     * @param serviceId service identifier
     * @param instanceId instance identifier
     * @return completion signal when gateway responds
     */
    public Future<Void> deregisterInstance(String serviceId, String instanceId) {
        return webClient.request(new RequestOptions().setMethod(HttpMethod.DELETE).setAbsoluteURI(baseUrl + "/registry/services/" + serviceId + "/instances/" + instanceId).setHeaders(MultiMap.caseInsensitiveMultiMap().add("Content-Type", "application/json")))
                .compose(HttpClientRequest::send)
                .compose(resp -> resp.body().mapEmpty());
    }
}
