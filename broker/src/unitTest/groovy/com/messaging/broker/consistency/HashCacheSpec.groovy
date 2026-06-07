package com.messaging.broker.consistency

import spock.lang.Specification

class HashCacheSpec extends Specification {

    def "cached hash bytes cannot be mutated by callers"() {
        given:
        def cache = new HashCache(64, 1000)
        def key = new HashCache.Key("prices-v1", 0L, 10L, "default")
        byte[] source = [1, 2, 3] as byte[]

        when:
        def cached = cache.getOrCompute(key) {
            new HashCache.CachedHash(source, 3L, "default")
        }
        source[0] = 9
        def exposed = cached.hash
        exposed[1] = 9

        then:
        cache.getOrCompute(key) {
            throw new AssertionError("cache miss")
        }.hash == ([1, 2, 3] as byte[])
    }

    def "recursive computation of the same key fails instead of deadlocking"() {
        given:
        def cache = new HashCache(64, 1000)
        def key = new HashCache.Key("prices-v1", 0L, 10L, "default")

        when:
        cache.getOrCompute(key) {
            cache.getOrCompute(key) {
                new HashCache.CachedHash(new byte[]{1}, 1L, "default")
            }
        }

        then:
        def failure = thrown(IllegalStateException)
        failure.message.contains("Recursive hash computation")
    }
}
