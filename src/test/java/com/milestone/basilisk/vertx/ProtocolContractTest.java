package com.milestone.basilisk.vertx;

import io.vertx.core.json.JsonObject;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ProtocolContractTest {
    @Test
    void forwardRequestSerializesCorrectly() {
        var req = new BusClient.ForwardRequest("orders", "order.query", new JsonObject().put("orderId", "42"), 3000L);
        var wire = new JsonObject()
            .put("type", ProtocolTypes.FORWARD)
            .put("forwardRequest", new JsonObject()
                .put("targetServiceId", req.targetServiceId())
                .put("messageType", req.messageType())
                .put("payload", req.payload())
                .put("timeoutMs", req.timeoutMs()));

        Assertions.assertEquals(ProtocolTypes.FORWARD, wire.getString("type"));
        var fr = wire.getJsonObject("forwardRequest");
        Assertions.assertEquals("orders", fr.getString("targetServiceId"));
        Assertions.assertEquals("order.query", fr.getString("messageType"));
        Assertions.assertEquals("42", fr.getJsonObject("payload").getString("orderId"));
        Assertions.assertEquals(3000L, fr.getLong("timeoutMs"));
    }

    @Test
    void clientConfigHoldsExpectedValues() {
        var config = new BasiliskClient.BasiliskClientConfig(
            "http://127.0.0.1:3000",
            "127.0.0.1",
            5090,
            "orders",
            "fp-orders",
            List.of("/api/orders"),
            "http",
            "127.0.0.1",
            7001,
            1,
            "token",
            "secret-token"
        );

        Assertions.assertEquals("orders", config.serviceId());
        Assertions.assertEquals(List.of("/api/orders"), config.pathPrefixes());
        Assertions.assertEquals(5090, config.busPort());
    }
}
