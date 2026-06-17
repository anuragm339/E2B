package com.messaging.broker.monitoring

import spock.lang.Specification

/**
 * The what/how/why mapping that makes the error API read as plain English: the human description,
 * the surfacing component+type, and the underlying reason (ErrorCode metadata + known causes).
 */
class ErrorExplainerSpec extends Specification {

    def "what uses an embedded [CODE] as the identity, else the human message"() {
        expect: "the [CODE] is the 'what' (its params become the 'why')"
        ErrorExplainer.what(null, "io.micronaut.http.client.exceptions.HttpClientException",
                "[REGISTRY_TOPOLOGY_FETCH_FAILED] category=REGISTRY, code=6002, retriable=true") ==
                "REGISTRY_TOPOLOGY_FETCH_FAILED"

        and: "plain message passes through"
        ErrorExplainer.what(null, "java.net.UnknownHostException", "Failed to connect to remote") ==
                "Failed to connect to remote"

        and: "falls back to errorCode then exception simple-name"
        ErrorExplainer.what("DATA_REFRESH_REPLAY_FAILED", null, null) == "DATA_REFRESH_REPLAY_FAILED"
        ErrorExplainer.what(null, "java.io.IOException", null) == "IOException"
    }

    def "how names the surfacing component and exception type (simple names)"() {
        expect:
        ErrorExplainer.how("com.messaging.broker.core.CloudRegistryClient",
                "io.micronaut.http.client.exceptions.ReadTimeoutException") ==
                "surfaced as ReadTimeoutException in CloudRegistryClient"

        and: "nested exception class uses the inner simple name"
        ErrorExplainer.how("com.messaging.pipe.HttpPipeConnector",
                "io.netty.channel.AbstractChannel\$AnnotatedNoRouteToHostException") ==
                "surfaced as AnnotatedNoRouteToHostException in HttpPipeConnector"

        and: "no exception -> logged by component"
        ErrorExplainer.how("com.messaging.broker.legacy.LegacyConsumerDeliveryManager", null) ==
                "logged by LegacyConsumerDeliveryManager"
    }

    def "why combines ErrorCode metadata with a known cause for the exception type"() {
        expect: "DNS failure explained"
        ErrorExplainer.why(null, "java.net.UnknownHostException", "Failed to connect to remote")
                .contains("host name could not be resolved")

        and: "timeout explained"
        ErrorExplainer.why(null, "io.micronaut.http.client.exceptions.ReadTimeoutException", "x")
                .contains("did not respond within the timeout")

        and: "category/retriable parsed from the message"
        def why = ErrorExplainer.why(null, "io.micronaut.http.client.exceptions.HttpClientException",
                "[REGISTRY_TOPOLOGY_FETCH_FAILED] category=REGISTRY, retriable=true")
        why.contains("category=REGISTRY")
        why.contains("retriable=true")

        and: "unknown -> see message"
        ErrorExplainer.why(null, "com.example.WeirdException", "boom") == "see message"
    }
}
