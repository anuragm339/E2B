package com.messaging.broker.systemtest.journey

import com.messaging.broker.snapshot.HttpBootstrapSourceClient
import com.messaging.broker.systemtest.support.TwoBrokerJourneySupport
import com.messaging.common.api.StorageEngine

/**
 * Two-broker journey for the incremental (no-snapshot) download-refresh path: the child pulls ALL
 * topics from the parent with the real {@link HttpBootstrapSourceClient} over HTTP, ingesting into
 * its OWN storage. Asserts the k-way merge delivers records across topics with disjoint offset
 * ranges, and that re-pulling never duplicates.
 */
class IncrementalPullJourneySpec extends TwoBrokerJourneySupport {

    def "child k-way-merge-pulls all topics from the parent over HTTP, with no duplicates"() {
        given: "parent storage holds records across two topics with disjoint offset ranges"
        def parentStorage = parentBean(StorageEngine)
        append(parentStorage, 'prices-v1', [10000L, 10002L, 11000L])
        append(parentStorage, 'reference-data-v5', [20000L, 21000L])

        when: "the child pulls everything from the parent over HTTP"
        def childClient = childBean(HttpBootstrapSourceClient)
        childClient.bulkFetchFromParent(parentUrl(), childDir.toString())

        then: "the child's storage now holds exactly the parent's records (k-way merge, all topics)"
        def childStorage = childBean(StorageEngine)
        offsetsOf(childStorage, 'prices-v1') == [10000L, 10002L, 11000L]
        offsetsOf(childStorage, 'reference-data-v5') == [20000L, 21000L]

        when: "a brand-new record arrives on the parent and the child pulls again"
        append(parentStorage, 'prices-v1', [12000L])
        childClient.bulkFetchFromParent(parentUrl(), childDir.toString())

        then: "only the new record is added — earlier records are NOT duplicated"
        offsetsOf(childStorage, 'prices-v1') == [10000L, 10002L, 11000L, 12000L]
        offsetsOf(childStorage, 'reference-data-v5') == [20000L, 21000L]
    }
}
