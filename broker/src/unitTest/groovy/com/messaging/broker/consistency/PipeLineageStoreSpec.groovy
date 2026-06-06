package com.messaging.broker.consistency

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class PipeLineageStoreSpec extends Specification {

    @TempDir
    Path tempDir

    def "seedIfEmpty installs a single open row only when empty"() {
        given:
        def store = new PipeLineageStore(tempDir)

        when:
        store.seedIfEmpty('http://parent-1:9092')
        store.seedIfEmpty('http://parent-2:9092') // should be ignored
        def current = store.currentLineage()
        def all = store.allEntries()

        then:
        current.parentUrl == 'http://parent-1:9092'
        current.offsetStart == 0L
        current.open
        all.size() == 1

        cleanup:
        store?.close()
    }

    def "recordParentSwitch closes the open row and opens a new one"() {
        given:
        def store = new PipeLineageStore(tempDir)
        store.seedIfEmpty('http://parent-1:9092')

        when:
        store.recordParentSwitch('http://parent-2:9092', 100L)
        def all = store.allEntries()
        def current = store.currentLineage()

        then:
        all.size() == 2
        all[0].parentUrl == 'http://parent-1:9092'
        all[0].offsetStart == 0L
        all[0].offsetEndExclusive == 100L
        all[0].maxOffsetInclusive() == 99L

        current.parentUrl == 'http://parent-2:9092'
        current.offsetStart == 100L
        current.open

        cleanup:
        store?.close()
    }

    def "resolve splits a range across multiple lineage eras"() {
        given:
        def store = new PipeLineageStore(tempDir)
        store.seedIfEmpty('http://parent-1:9092')
        store.recordParentSwitch('http://parent-2:9092', 100L)
        store.recordParentSwitch('http://parent-3:9092', 250L)

        when:
        def sub = store.resolve(50L, 300L)

        then: "expect three subranges: [50..99] p1, [100..249] p2, [250..300] p3"
        sub.size() == 3
        sub[0].fromOffsetInclusive == 50L
        sub[0].toOffsetInclusive == 99L
        sub[0].parentUrl == 'http://parent-1:9092'

        sub[1].fromOffsetInclusive == 100L
        sub[1].toOffsetInclusive == 249L
        sub[1].parentUrl == 'http://parent-2:9092'

        sub[2].fromOffsetInclusive == 250L
        sub[2].toOffsetInclusive == 300L
        sub[2].parentUrl == 'http://parent-3:9092'

        cleanup:
        store?.close()
    }

    def "resolve returns single subrange when entirely within one era"() {
        given:
        def store = new PipeLineageStore(tempDir)
        store.seedIfEmpty('http://parent-1:9092')
        store.recordParentSwitch('http://parent-2:9092', 100L)

        when:
        def sub = store.resolve(120L, 130L)

        then:
        sub.size() == 1
        sub[0].parentUrl == 'http://parent-2:9092'
        sub[0].fromOffsetInclusive == 120L
        sub[0].toOffsetInclusive == 130L

        cleanup:
        store?.close()
    }

    def "recordParentSwitch on empty table just inserts the new row"() {
        given:
        def store = new PipeLineageStore(tempDir)

        when:
        store.recordParentSwitch('http://parent-1:9092', 0L)
        def current = store.currentLineage()

        then:
        current.parentUrl == 'http://parent-1:9092'
        current.offsetStart == 0L

        cleanup:
        store?.close()
    }
}
