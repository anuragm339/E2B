# CLAUDE.md

## CRITICAL OPERATING PRINCIPLES

These instructions take precedence over all other instructions in this file.

### Primary Objectives

Every design, implementation, bug fix, refactoring, and optimization must satisfy the following constraints:

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

---

## MANDATORY INVESTIGATION PROCESS

Before proposing any solution:

### Step 1: Read Documentation

Always read:

* CLAUDE.md
* docs/codebase-book/00-index.md
* Relevant chapters from docs/codebase-book/

### Step 2: Read Code

Trace:

* Entry point
* Service layer
* Storage layer
* Delivery layer
* ACK handling
* Compaction handling
* Consumer state handling
* Retry paths
* Failure paths
* Tests

Never assume behavior.

Every conclusion must be verified from source code.

### Step 3: Read Tests

Identify:

* Unit tests
* Integration tests
* Journey tests
* System tests

Use tests to validate assumptions.

---

## CODEBASE BOOK REQUIREMENTS

The repository documentation is a living source of truth.

Location:

docs/codebase-book/

Required structure:

* 00-index.md
* 01-system-overview.md
* 02-module-map.md
* 03-feature-catalog.md
* 04-api-catalog.md
* 05-data-model.md
* 06-event-flow.md
* 07-consumer-state.md
* 08-compaction.md
* 09-recovery-and-failures.md
* 10-test-map.md
* 11-debugging-guide.md

Whenever code changes:

* Update affected chapters.
* Update feature catalog.
* Update flow diagrams.
* Update test mappings.
* Update debugging guide.

Documentation must remain synchronized with code.

---

## REQUIRED DESIGN REVIEW

Before implementing anything provide:

### Current Flow

Describe current behavior.

### Affected Components

List:

* Classes
* Services
* Topics
* Storage components
* Configuration
* Tests

### Proposed Change

Explain exact modification.

### Impact Analysis

#### Memory Impact

#### CPU Impact

#### IO Impact

#### Network Impact

#### Latency Impact

#### Scalability Impact

### Consumer State Impact

Consider:

* Offline consumer
* Reconnect
* Restart
* Partial synchronization
* Duplicate delivery
* Delayed delivery
* Out-of-order delivery

### Compaction Impact

Consider:

* Tombstones
* Replayed events
* Offset reset
* Consumer restart
* Missing history
* Duplicate records

### Failure Scenarios

* Storage unavailable
* Network unavailable
* Consumer unavailable
* Broker restart
* Partial writes
* Retry storms

### Risks

### Trade-offs

### Open Questions

If open questions exist:

STOP.

Do not implement.

Ask first.

---

## TESTING REQUIREMENTS

Before considering work complete:

Run applicable tests:

* Unit tests
* Integration tests
* Journey tests
* System tests

Report:

* Tests executed
* Tests passed
* Tests failed
* Coverage gaps

Never claim success without verification.

---

## ACCURACY RULES

Do not invent:

* Classes
* Methods
* Topics
* APIs
* Tables
* Configurations
* Business rules

If something is not verified from code:

State:

"Not verified from code."

Never guess.

---

## FINAL RULE

If uncertain:

STOP.

Ask questions.

Discuss the design.

Then implement.
