package com.messaging.broker.snapshot

import spock.lang.Specification

class RefreshTypeSpec extends Specification {

    def "from() parses known values case-insensitively and defaults to DOWNLOAD"() {
        expect:
        RefreshType.from(raw) == expected

        where:
        raw            || expected
        "LOCAL"        || RefreshType.LOCAL
        "local"        || RefreshType.LOCAL
        " Snapshot "   || RefreshType.SNAPSHOT
        "INCREMENTAL"  || RefreshType.INCREMENTAL
        "cloud"        || RefreshType.CLOUD
        "DOWNLOAD"     || RefreshType.DOWNLOAD
        null           || RefreshType.DOWNLOAD
        ""             || RefreshType.DOWNLOAD
        "nonsense"     || RefreshType.DOWNLOAD
    }

    def "forcedSource maps types to bootstrap sources (null = auto / local)"() {
        expect:
        RefreshType.SNAPSHOT.forcedSource() == BootstrapSource.SNAPSHOT
        RefreshType.INCREMENTAL.forcedSource() == BootstrapSource.INCREMENTAL_PARENT
        RefreshType.CLOUD.forcedSource() == BootstrapSource.CLOUD
        RefreshType.DOWNLOAD.forcedSource() == null
        RefreshType.LOCAL.forcedSource() == null
    }

    def "isLocal is true only for LOCAL"() {
        expect:
        RefreshType.LOCAL.isLocal()
        !RefreshType.DOWNLOAD.isLocal()
        !RefreshType.CLOUD.isLocal()
    }
}
