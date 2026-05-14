# vertx-client

Vert.x client library for Milestone Basilisk gateway and service bus.

## Features

- Register a service instance, receive a generated instance ID, and establish an authenticated service bus connection in one workflow
- Subscribe/unsubscribe to topics and receive events
- Publish events with structured payloads
- Send forward requests and wait for forward responses
- Register request responders with `bus.onRequest(messageType, handler)`
- Call gateway registry APIs to register/deregister service instances
- Use `BasiliskClient` as the top-level client with `gateway` and `bus` handles

## Quick usage

```java
Vertx vertx = Vertx.vertx();
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
    "instance-token"
)).onSuccess(client -> {
    System.out.println("instance id: " + client.instanceId);
});
```

`BasiliskClient` owns a `gateway` client for registry operations and a `bus` client for
the TCP service bus. The top-level connect flow automatically registers the instance,
accepts the generated instance ID returned by the registry, and then opens the bus
connection using the issued token.

## Notes on low-level clients

- `BusClient.connect(...)` authenticates during the `connect` handshake.
- `GatewayApiClient.registerInstance(...)` responses include both `instanceId` and `token`.
- `GatewayApiClient.registerInstanceAuto(...)` sends an empty instance ID so the registry generates one.

## End-to-end test

```bash
mvn test
```
