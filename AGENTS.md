# AGENTS.md

Operational guidance for human contributors and LLM/code agents working in this repository.

## 1. Purpose

Work on the Vert.x Basilisk client safely without breaking core contracts:

- Gateway registration API compatibility
- Service bus protocol compatibility
- Request/response correlation semantics
- Public API stability for consumers

Basilisk belongs to **Milestone**. Use `com.milestone...` naming for ownership/package identifiers.

## 2. Architecture Map

- `src/main/java/com/milestone/basilisk/vertx/BasiliskClient.java`: top-level connect flow and composed client
- `src/main/java/com/milestone/basilisk/vertx/GatewayApiClient.java`: HTTP service registry operations via Vert.x WebClient
- `src/main/java/com/milestone/basilisk/vertx/BusClient.java`: TCP service bus lifecycle, subscriptions, requests
- `src/main/java/com/milestone/basilisk/vertx/ProtocolTypes.java`: wire contract records and helpers
- `src/test/java/com/milestone/basilisk/vertx/ProtocolContractTest.java`: protocol contract integration tests
- `src/test/java/com/milestone/basilisk/vertx/FullFeatureE2eTest.java`: full feature end-to-end tests

## 3. Non-Negotiable Contracts

1. **Registration-first connect**: top-level connect must register and then authenticate bus connect with issued `instanceId` and `token`.
2. **Generated identity support**: auto-register must send empty instance ID and accept generated identity from gateway.
3. **Bus framing**: newline-delimited JSON messages over TCP.
4. **Field compatibility**: preserve wire/json field names used by other Basilisk clients.
5. **Forward flow semantics**: request/response correlation IDs must remain stable and unique.

## 4. Change Strategy

When making changes:

- Prefer small, focused commits.
- Preserve backward compatibility for public API names and protocol fields.
- If compatibility must change, update tests and `README.md` in the same change.
- Refactor large functions into helpers to reduce cognitive complexity.

## 5. Testing Policy

Minimum validation for non-trivial changes:

```bash
mvn test
```

If your change touches gateway registration, bus protocol handling, or request routing, add/update integration tests in `src/test/java`.

## 6. Documentation Policy

Update docs when the behavior changes:

- `README.md` for APIs, configuration, and flow behavior
- Javadoc for public types/functions where behavior is not obvious
- keep examples copy-paste runnable
- for any new or changed public API or protocol field, documentation updates are REQUIRED in the same change

Avoid release/change-log style narrative in `README.md`.

## 7. Common Pitfalls

- Blocking event-loop threads with synchronous IO
- Breaking newline framing by writing partial frames
- Changing JSON field names without compatibility updates
- Losing pending request cleanup on timeout/connection close

## 8. Contribution Etiquette

- Do not rewrite unrelated code.
- Do not remove tests without replacement coverage.
- Keep naming explicit and domain-specific.
- If uncertain about behavior, add tests first, then implement.
