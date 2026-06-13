# Glossary

Related: [Overview](01-system-overview.md), [Data model](05-data-model.md), [Events](06-event-kafka-flow.md), [POS state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md).

- **ACK store:** Per-record consumption audit stored in RocksDB, separate from the committed consumer offset.
- **Active segment:** Writable current segment for a topic/partition.
- **Batch ACK:** Consumer confirmation that a delivered batch was decoded/processed.
- **Compaction checkpoint:** Highest sealed range already processed for a topic.
- **Compaction index:** Latest known offset/timestamp per topic/message key plus stale-offset watermark.
- **Consumer group:** Logical independent progress owner for a topic.
- **ConsumerKey:** `clientId + topic + group`; identifies one live socket registration.
- **DeliveryBatch:** Transfer object exposing record count, byte count, offsets, and `transferTo`.
- **DeliveryKey:** `group + topic`; identifies logical in-flight and committed delivery state.
- **File region:** Netty transfer object used to send segment bytes without copying through a broker heap buffer.
- **Group commit:** Periodic force of active storage rather than force per append.
- **Journey test:** In-process end-to-end broker/cloud/consumer test from `broker/src/systemTest`, run by `journeyTest`.
- **Legacy client:** Event-protocol consumer that registers a service name and may subscribe to multiple mapped topics over one connection.
- **Modern client:** BrokerMessage-protocol consumer, normally one TCP connection per topic/group.
- **Next-to-deliver offset:** Modern offset value immediately after the last acknowledged original batch offset.
- **Parent:** Upstream broker/cloud HTTP endpoint selected by topology.
- **Partition:** Storage subdivision. Production code in this repository uses partition `0`.
- **Pipe:** HTTP polling replication link from parent to local broker.
- **POS:** Point-of-sale/store terminal context. Represented through consumer connection/readiness/refresh state, not a dedicated entity.
- **READY:** Broker control event opening normal delivery or ending refresh.
- **RESET:** Broker control event asking a consumer to clear/reinitialize its local view before replay.
- **Sealed segment:** Read-only segment eligible for compaction.
- **Sparse offsets:** Stored offsets with gaps, expected after pipe replication or compaction.
- **Stale record:** Earlier topic/key version superseded by a later offset.
- **System test:** Separate-process black-box test from `broker/src/blackboxSystemTest`.
- **Tombstone:** Latest keyed DELETE record retained temporarily to propagate deletion.
- **Topic fair scheduler:** Scheduler limiting concurrent delivery work per topic and pending retry per delivery key.
- **Watermark:** Latest known storage offset used to avoid no-data delivery reads.
- **Zero-copy:** Broker transfer of file-backed segment bytes through Netty `FileRegion`; client still decodes records into objects.

Source definitions are primarily in `common/src/main/java/com/messaging/common/model/`, `broker/src/main/java/com/messaging/broker/model/`, storage segment classes, and consumer/compaction packages.
