# Repository Guidelines

These instructions define how agents should investigate, design, implement, test, and document changes in this repository.

## Critical Operating Principles

Every design, bug fix, refactor, optimization, and implementation must satisfy these constraints:

1. Do not significantly increase memory usage.
2. Do not significantly increase CPU usage.
3. Do not significantly increase disk IO.
4. Do not introduce expensive network operations.
5. Do not impact the existing message flow.
6. Do not break backward compatibility.
7. Handle consumer/POS state transitions safely.
8. Handle compaction scenarios safely.
9. Prefer simple solutions over complex solutions.
10. If information is missing, stop and ask questions.
11. Always throw the project's own `MessagingException` or a subclass. Never throw bare JDK exceptions from production code.

## Mandatory Investigation Process

Before proposing or implementing a solution, read the relevant documentation and verify behavior from source code.

Read:

- `AGENTS.md`
- `CLAUDE.md`, if present
- `docs/codebase-book/00-index.md`
- Relevant chapters from `docs/codebase-book/`

Trace the full affected flow:

- Entry point
- Service layer
- Storage layer
- Delivery layer
- ACK handling
- Compaction handling
- Consumer state handling
- Retry paths
- Failure paths
- Tests

Never assume behavior. Every conclusion must be verified from code or reported as not verified.

## Codebase Book Requirements

The documentation under `docs/codebase-book/` is a living source of truth. When code behavior changes, update the affected chapters, feature catalog, flow diagrams, test mappings, and debugging guidance.

After editing any `docs/codebase-book/NN-*.md` chapter, regenerate the rendered site:

```bash
node docs/codebase-book/build-site.mjs
```

Commit the regenerated `docs/codebase-book/site/assets/content.js` and any other changed generated site files together with the markdown updates.

## Required Design Review

Before implementation, provide or internally verify:

- Current flow: describe existing behavior.
- Affected components: classes, services, topics, storage components, configuration, and tests.
- Proposed change: explain the exact modification.
- Impact analysis: memory, CPU, IO, network, latency, and scalability.
- Consumer state impact: offline consumers, reconnect, restart, partial synchronization, duplicate delivery, delayed delivery, and out-of-order delivery.
- Compaction impact: tombstones, replayed events, offset reset, consumer restart, missing history, and duplicate records.
- Failure scenarios: storage unavailable, network unavailable, consumer unavailable, broker restart, partial writes, and retry storms.
- Risks, trade-offs, and open questions.

If open questions affect correctness, stop and ask before implementing.

## Testing Requirements

Before considering work complete, run the applicable test level and report the result.

Common commands:

```bash
./gradlew test
./gradlew :broker:test
./gradlew :broker:integrationTest
./gradlew :broker:journeyTest
./gradlew :broker:systemTest
```

Report:

- Tests executed
- Tests passed
- Tests failed
- Coverage gaps

Never claim success without verification.

## Exception Handling Rules

Production code must raise project exceptions, not bare JDK exceptions such as `RuntimeException`, `IllegalArgumentException`, `IllegalStateException`, or `UnsupportedOperationException`.

Use:

- `MessagingException`
- `StorageException`
- `NetworkException`
- `ConsumerException`
- `DataRefreshException`

Rules:

- Use the most specific `ErrorCode`.
- Use `VALIDATION_INVALID_ARGUMENT` for generic argument validation.
- Use `BROKER_INVALID_STATE` for lifecycle or state guards.
- Add a new `ErrorCode` only when no existing one fits.
- Attach debugging context with `withContext(...)`.
- When a logger is available, throw via `ExceptionLogger.logAndThrow(log, ex)`.
- Pure value objects or validators without a logger may throw directly.
- When changing thrown exceptions, update matching `catch` blocks in lockstep.
- Exceptions are Java-internal only and are never serialized over the wire.

`MessagingException` is unchecked and must remain unchecked to avoid API churn and hot-path decode overhead.

## Accuracy Rules

Do not invent:

- Classes
- Methods
- Topics
- APIs
- Tables
- Configurations
- Business rules

If something is not verified from code, say: `Not verified from code.`

## Project Structure

This is a Gradle multi-module Java 17/Micronaut messaging provider. Modules are declared in `settings.gradle`:

- `common`: shared models, contracts, exceptions, and annotations.
- `storage`: segment storage, metadata, recovery, and batch production.
- `network`: TCP transport, codecs, protocol handlers, and legacy protocol support.
- `pipe`: parent-broker polling and upstream replication.
- `broker`: orchestration, delivery, refresh, topology, admin APIs, and metrics.
- `client`: client-side connection and consumer management.
- `test-consumer`: support app for journey and system tests.

## Build and Development Commands

Use Gradle from the repository root:

```bash
./gradlew build
./gradlew :broker:run
./gradlew jacocoReport
```

Runtime defaults are in `broker/src/main/resources/application.yml`. Common overrides include `DATA_DIR`, `BROKER_PORT`, `HTTP_PORT`, `NODE_ID`, and `REGISTRY_URL`.

## Coding Style

Follow the existing Java/Groovy style: 4-space indentation, package names under `com.messaging.*`, descriptive domain names, and narrow module boundaries. Keep broker orchestration out of storage and network contracts. Qodana is configured through `qodana.yaml`; no local Checkstyle or Spotless Gradle task is currently configured.

## Commit and Pull Request Guidelines

Recent commits use Conventional Commit-style subjects, such as:

- `fix(refresh): await READY, reconcile snapshot deletions`
- `feat(refresh): type-driven refresh API`
- `test(refresh): full orchestrator end-to-end journey`
- `docs(codebase-book): regenerate site for chapter 20`

Keep commits scoped by subsystem. Pull requests should describe behavior changes, affected modules, config/data impact, docs updates, and exact tests run.

## Final Rule

If uncertain, stop, ask questions, discuss the design, and then implement.
