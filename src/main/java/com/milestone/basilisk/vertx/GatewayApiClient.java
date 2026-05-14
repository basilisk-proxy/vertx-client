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

@SuppressWarnings("unused")
public class GatewayApiClient {
    private final String baseUrl;
    private final HttpClientAgent webClient;

    public GatewayApiClient(Vertx vertx, String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.webClient = vertx.createHttpClient();
    }

    public Future<JsonObject> registerInstanceAuto(JsonObject request) {
        var copy = Objects.requireNonNullElse(request.copy(), new JsonObject());
        copy.getJsonObject("instance").put("instanceId", "");
        return registerInstance(copy);
    }

    public Future<JsonObject> registerInstance(JsonObject request) {
        return webClient.request(new RequestOptions().setMethod(HttpMethod.POST).setAbsoluteURI(baseUrl + "/registry/register").setHeaders(MultiMap.caseInsensitiveMultiMap().add("Content-Type", "application/json")))
            .compose(httpClientRequest -> httpClientRequest.send(request.encode()))
            .compose(resp -> resp.body().map(Buffer::toJsonObject));
    }

    public Future<Void> deregisterInstance(String serviceId, String instanceId) {
        return webClient.request(new RequestOptions().setMethod(HttpMethod.DELETE).setAbsoluteURI(baseUrl + "/registry/services/" + serviceId + "/instances/" + instanceId).setHeaders(MultiMap.caseInsensitiveMultiMap().add("Content-Type", "application/json")))
                .compose(HttpClientRequest::send)
                .compose(resp -> resp.body().mapEmpty());
    }
}
