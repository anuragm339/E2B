package com.messaging.broker.monitoring

import spock.lang.Specification

/**
 * Covers the level-aware ring features added for the trace-aware error API: ERROR-by-default
 * min-level filtering, top-unique grouping with a sample traceId, and the chronological per-trace
 * chain.
 */
class ErrorRecorderSpec extends Specification {

    ErrorRecorder rec = new ErrorRecorder()

    private void put(long ts, String level, String logger, String msg, String code, String exClass, String traceId) {
        rec.record(ts, level, logger, msg, code, exClass, null, traceId, null, null, null)
    }

    def "recent() treats level as a MINIMUM: ERROR excludes WARN, WARN includes both"() {
        given:
        put(1, "ERROR", "a", "boom", "REGISTRY_TOPOLOGY_FETCH_FAILED", null, "t1")
        put(2, "WARN", "b", "warned", null, null, "t2")

        expect: "default-style ERROR view shows only the ERROR"
        rec.recent("ERROR", null, null, 0, 100)*.level == ["ERROR"]

        and: "WARN view shows both (most-recent-first)"
        rec.recent("WARN", null, null, 0, 100)*.level == ["WARN", "ERROR"]

        and: "null min-level = no filter"
        rec.recent(null, null, null, 0, 100).size() == 2
    }

    def "topUnique() collapses the same error across different exception classes into one count"() {
        given: "same 'Failed to connect' message logged as 3 different exception classes"
        put(10, "ERROR", "pipe", "Failed to connect to remote", null, "java.net.UnknownHostException", "tA")
        put(11, "ERROR", "pipe", "Failed to connect to remote", null, "java.net.UnknownHostException", "tB")
        put(12, "ERROR", "pipe", "Failed to connect to remote", null, "io.netty.channel.AbstractChannel\$AnnotatedNoRouteToHostException", "tC")

        and: "one ErrorCode surfacing as two different HTTP client exceptions (code embedded + varying timestamp)"
        put(13, "ERROR", "reg", "[REGISTRY_TOPOLOGY_FETCH_FAILED] code=6002 timestamp=2026-06-16T19:20:40.983846006Z", null, "io.micronaut.http.client.exceptions.ReadTimeoutException", "tD")
        put(14, "ERROR", "reg", "[REGISTRY_TOPOLOGY_FETCH_FAILED] code=6002 timestamp=2026-06-16T19:20:59.988560126Z", null, "io.micronaut.http.client.exceptions.HttpClientException", "tE")

        and: "a benign WARN"
        put(15, "WARN", "leg", "benign", null, null, "tW")

        when: "ERROR view (default)"
        def top = rec.topUnique("ERROR", 10)

        then: "just TWO unique errors — connectivity collapsed to one, registry collapsed to one"
        top.size() == 2
        top[0].error == "Failed to connect to remote"
        top[0].count == 3L
        top[0].firstSeen == 10L
        top[0].lastSeen == 12L
        top[0].sampleTraceId == "tC"                       // latest occurrence
        top[0].what == "Failed to connect to remote"       // human "what"
        top[0].how.startsWith("surfaced as")               // how/where it surfaced
        top[0].why != null                                 // a reason is given
        top[1].error == "REGISTRY_TOPOLOGY_FETCH_FAILED"   // grouped by the [CODE] prefix
        top[1].count == 2L

        when: "WARN view adds the benign one"
        def withWarn = rec.topUnique("WARN", 10)

        then:
        withWarn.size() == 3
    }

    def "trace() returns one traceId's entries in chronological order (start -> end)"() {
        given: "two traces interleaved in arrival order"
        put(100, "WARN", "x", "reset retry", null, null, "trace-1")
        put(101, "ERROR", "y", "other error", null, null, "trace-2")
        put(102, "ERROR", "x", "refresh aborted", "DATA_REFRESH_REPLAY_FAILED", null, "trace-1")

        when: "include warnings so the whole chain shows"
        def chain = rec.trace("trace-1", "WARN")

        then: "only trace-1, oldest first"
        chain*.ts == [100L, 102L]
        chain*.message == ["reset retry", "refresh aborted"]

        and: "ERROR-only view drops the leading WARN"
        rec.trace("trace-1", "ERROR")*.ts == [102L]

        and: "unknown trace is empty"
        rec.trace("nope", "WARN").isEmpty()
    }
}
