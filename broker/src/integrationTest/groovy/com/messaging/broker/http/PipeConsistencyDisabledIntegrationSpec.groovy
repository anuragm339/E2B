package com.messaging.broker.http

import com.messaging.broker.support.BrokerHttpSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest

/**
 * Fail-closed contract: with pipe.consistency.enabled=false (the default) the served
 * endpoints answer 404 — indistinguishable from an older build, which children classify
 * as UNSUPPORTED_PARENT — and the admin trigger answers 503.
 */
@MicronautTest
class PipeConsistencyDisabledIntegrationSpec extends BrokerHttpSpecSupport {

    def "all consistency endpoints are dark by default"() {
        expect:
        get('/pipe/consistency/digest?topic=t&watermark=1&buckets=8').statusCode() == 404
        get('/pipe/consistency/bucket?topic=t&watermark=1&buckets=8&bucket=0').statusCode() == 404
        post('/pipe/consistency/classify', [topic: 't', watermark: 1, offsets: [], keys: []]).statusCode() == 404
        post('/admin/pipe-consistency/check', [:]).statusCode() == 503
    }
}
