package com.messaging.common.http

import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

@MicronautTest(startApplication = false)
class AuthTokenClientFilterDisabledIntegrationSpec extends Specification {

    @Inject ApplicationContext context

    def "filter bean is not created when auth token is missing"() {
        expect:
        !context.containsBean(AuthTokenClientFilter)
    }
}
