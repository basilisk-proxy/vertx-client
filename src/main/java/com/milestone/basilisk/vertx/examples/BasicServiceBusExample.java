package com.milestone.basilisk.vertx.examples;

import com.milestone.basilisk.vertx.BasiliskClient;
import com.milestone.basilisk.vertx.BusClient.ForwardRequest;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.List;

public class BasicServiceBusExample {
    public static void main(String[] args) {
        var vertx = Vertx.vertx();
        BasiliskClient.connect(vertx, new BasiliskClient.BasiliskClientConfig(
            "http://127.0.0.1:3000",
            "127.0.0.1",
            5090,
            "orders",
            "orders-v1",
            List.of("/api/orders"),
            "http",
            "127.0.0.1",
            7001,
            1,
            "token",
            "replace-me"
        )).compose(client -> client.bus.forward(new ForwardRequest("orders", "order.query", new JsonObject(), 3000L))
            .onSuccess(response -> System.out.println("Forward response type: " + response.getString("messageType")))
        ).onFailure(err -> {
            err.printStackTrace();
            vertx.close();
        });
    }
}
