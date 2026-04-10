package com.messaging.common.http

import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.filter.ClientFilterChain
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

@MicronautTest(startApplication = false)
class AuthTokenClientFilterIntegrationSpec extends Specification implements TestPropertyProvider {

    @Inject AuthTokenClientFilter filter

    @Override
    Map<String, String> getProperties() {
        [
            'broker.http.auth.token' : 'secret-token',
            'broker.http.auth.header': 'Authorization',
            'broker.http.auth.scheme': 'Bearer'
        ]
    }

    def "filter bean is created when auth token is configured"() {
        expect:
        filter != null
    }

    def "filter adds the configured auth header to outgoing requests"() {
        given:
        MutableHttpRequest<?> request = HttpRequest.GET('/pipe/poll')
        MutableHttpRequest<?> forwarded = null
        ClientFilterChain chain = Stub(ClientFilterChain) {
            proceed(_ as MutableHttpRequest<?>) >> { MutableHttpRequest<?> candidate ->
                forwarded = candidate
                Publishers.just(HttpResponse.ok())
            }
        }

        when:
        filter.doFilter(request, chain)

        then:
        forwarded == request
        request.headers.get('Authorization') == 'Bearer secret-token'
    }
}
