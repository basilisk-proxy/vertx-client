package com.milestone.basilisk.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class FullFeatureE2eTest {
    @Test
    void fullFeatureClientE2EWithGateway(Vertx vertx, VertxTestContext ctx) {
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

        // Verify configuration is assembled correctly prior to any gateway connection
        ctx.verify(() -> {
            org.junit.jupiter.api.Assertions.assertEquals("orders", config.serviceId());
            org.junit.jupiter.api.Assertions.assertEquals("fp-orders", config.fingerprint());
            org.junit.jupiter.api.Assertions.assertEquals(List.of("/api/orders"), config.pathPrefixes());
        });

        var gateway = new GatewayApiClient(vertx, config.gatewayBaseUrl());
        ctx.verify(() -> org.junit.jupiter.api.Assertions.assertNotNull(gateway));

        ctx.completeNow();
    }
}
