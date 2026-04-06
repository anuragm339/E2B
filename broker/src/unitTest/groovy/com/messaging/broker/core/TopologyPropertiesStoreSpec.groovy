package com.messaging.broker.core

import com.messaging.common.model.TopologyResponse
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class TopologyPropertiesStoreSpec extends Specification {

    @TempDir
    Path tempDir

    def "save and load topology properties"() {
        given:
        def store = new TopologyPropertiesStore(tempDir)
        def topology = new TopologyResponse()
        topology.setNodeId("node-1")
        topology.setRole(TopologyResponse.NodeRole.L2)
        topology.setRequestToFollow(["http://parent-1"])
        topology.setCloudDataUrl("http://cloud")
        topology.setCloudDataUrlFallback("http://cloud-fallback")
        topology.setTopologyVersion("v1")
        topology.setTopics(["t1", "t2"])

        when:
        store.saveTopology(topology)
        def props = store.loadTopology()

        then:
        props.getProperty("nodeId") == "node-1"
        props.getProperty("role") == "L2"
        props.getProperty("parentUrl") == "http://parent-1"
        props.getProperty("cloudDataUrl") == "http://cloud"
        props.getProperty("cloudDataUrlFallback") == "http://cloud-fallback"
        props.getProperty("topologyVersion") == "v1"
        props.getProperty("topics") == "t1,t2"

        and:
        store.getCurrentParentUrl() == "http://parent-1"
        store.getCurrentRole() == "L2"
    }
}
