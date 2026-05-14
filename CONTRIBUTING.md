# Contributing to Basilisk

This document is the contribution guide for humans and automation.

## Table of Contents

- [1. Scope](#1-scope)
- [2. Development Setup](#2-development-setup)
- [3. Project Rules](#3-project-rules)
- [4. Coding Standards](#4-coding-standards)
- [5. Testing Requirements](#5-testing-requirements)
- [6. Documentation Requirements](#6-documentation-requirements)
- [7. Pull Request Checklist](#7-pull-request-checklist)

## 1. Scope

This repository contains the Vert.x-based Java client for Milestone Basilisk:

- HTTP service registry client (Vert.x WebClient)
- TCP service bus client (Vert.x NetClient)
- Top-level connect flow that composes registry and bus clients

Contributions should align with those responsibilities.

## 2. Development Setup

### Prerequisites

- JDK 21+
- Maven 3.9+

### Common commands

```bash
mvn test
```

### Run example

```bash
mvn -q -DskipTests exec:java -Dexec.mainClass=examples.com.milestone.basilisk.vertx.BasicServiceBusExample
```

## 3. Project Rules

- Keep registration and bus connect handshake behavior compatible with other Basilisk clients.
- Preserve newline-delimited JSON framing and field names in protocol messages.
- Keep auto-registration semantics (empty instance ID → generated identity).
- Use Milestone ownership naming (`com.milestone...`) for new packages.

## 4. Coding Standards

- Keep classes focused; extract helpers when methods become difficult to follow.
- Prefer explicit names and small composable methods.
- Do not block Vert.x event-loop threads; isolate blocking operations.
- Add Javadoc on public APIs where behavior is not obvious.

## 5. Testing Requirements

- Add or update tests for behavior changes.
- Favor integration-style tests in `src/test/java` for public client behavior.
- Protocol changes should include success and error-path coverage.

## 6. Documentation Requirements

- Update `README.md` when behavior, API contracts, or examples change.
- For any new or changed public API or protocol field, documentation updates are mandatory in the same PR.
- Keep examples runnable and aligned with the current code.

## 7. Pull Request Checklist

Before opening a PR:

- [ ] Tests pass (`mvn test`)
- [ ] Tests cover public API behavior changes
- [ ] Documentation updated (`README.md`, inline Javadoc where needed)
- [ ] No unrelated file changes are included
