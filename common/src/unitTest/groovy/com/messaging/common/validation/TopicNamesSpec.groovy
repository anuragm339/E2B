package com.messaging.common.validation

import com.messaging.common.exception.ErrorCode
import com.messaging.common.exception.MessagingException
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Covers the topic-name validation reject path (P0 #5 path-traversal hardening). Topics become
 * filesystem path segments, so anything with a separator, parent ref, or empty/odd name must be
 * rejected — while every real topic still passes.
 */
class TopicNamesSpec extends Specification {

    @Unroll
    def "accepts valid topic name '#topic'"() {
        when:
        TopicNames.validate(topic)

        then:
        noExceptionThrown()

        where:
        topic << ['prices-v1', 'reference-data-v5', 'a', 'A1', 'topic.name',
                  'topic_name', 'colleague-card-pin-v2', 'x' * 255]
    }

    @Unroll
    def "rejects unsafe topic (#desc) with VALIDATION_INVALID_ARGUMENT"() {
        when:
        TopicNames.validate(topic)

        then:
        MessagingException e = thrown()
        e.errorCode == ErrorCode.VALIDATION_INVALID_ARGUMENT

        where:
        desc                 | topic
        'null'               | null
        'empty'              | ''
        'parent ref'         | '..'
        'absolute traversal' | '../../etc/passwd'
        'embedded traversal' | 'topic/../x'
        'forward slash'      | 'a/b'
        'back slash'         | 'a\\b'
        'leading dot'        | '.hidden'
        'leading dash'       | '-topic'
        'too long'           | 'x' * 256
    }
}
