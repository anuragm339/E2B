package com.messaging.broker.snapshot

import spock.lang.Specification

class RefreshTypeSpec extends Specification {

    def "from() parses known values case-insensitively and defaults to PIPE_AND_PROVIDER_REFRESH"() {
        expect:
        RefreshType.from(raw) == expected

        where:
        raw                                   || expected
        "LOCAL"                               || RefreshType.LOCAL
        "local"                               || RefreshType.LOCAL
        " Pipe_And_Provider_File_Download "   || RefreshType.PIPE_AND_PROVIDER_FILE_DOWNLOAD
        "PIPE_AND_PROVIDER_STREAM"            || RefreshType.PIPE_AND_PROVIDER_STREAM
        "cloud_sync"                          || RefreshType.CLOUD_SYNC
        "PIPE_AND_PROVIDER_REFRESH"           || RefreshType.PIPE_AND_PROVIDER_REFRESH
        "BARE_METAL"                          || RefreshType.BARE_METAL
        null                                  || RefreshType.PIPE_AND_PROVIDER_REFRESH
        ""                                    || RefreshType.PIPE_AND_PROVIDER_REFRESH
        "nonsense"                            || RefreshType.PIPE_AND_PROVIDER_REFRESH
    }

    def "forcedSource maps types to bootstrap sources (null = auto / local)"() {
        expect:
        RefreshType.PIPE_AND_PROVIDER_FILE_DOWNLOAD.forcedSource() == BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD
        RefreshType.PIPE_AND_PROVIDER_STREAM.forcedSource() == BootstrapSource.PIPE_AND_PROVIDER_STREAM
        RefreshType.CLOUD_SYNC.forcedSource() == BootstrapSource.CLOUD_SYNC
        RefreshType.PIPE_AND_PROVIDER_REFRESH.forcedSource() == null
        RefreshType.LOCAL.forcedSource() == null
    }

    def "isLocal is true only for LOCAL"() {
        expect:
        RefreshType.LOCAL.isLocal()
        !RefreshType.PIPE_AND_PROVIDER_REFRESH.isLocal()
        !RefreshType.CLOUD_SYNC.isLocal()
    }
}
