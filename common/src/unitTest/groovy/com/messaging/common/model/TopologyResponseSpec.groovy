package com.messaging.common.model

import spock.lang.Specification

class TopologyResponseSpec extends Specification {

    def "setters and getters preserve topology fields"() {
        given:
        def response = new TopologyResponse()

        when:
        response.nodeId = 'broker-l2-1'
        response.role = TopologyResponse.NodeRole.L2
        response.requestToFollow = ['http://parent-1', 'http://parent-2']
        response.cloudDataUrl = 'http://cloud-primary'
        response.cloudDataUrlFallback = 'http://cloud-fallback'
        response.topologyVersion = 'v42'
        response.topics = ['prices-v1', 'orders-v1']

        then:
        response.nodeId == 'broker-l2-1'
        response.role == TopologyResponse.NodeRole.L2
        response.requestToFollow.size() == 2
        response.cloudDataUrlFallback == 'http://cloud-fallback'
        response.topologyVersion == 'v42'
        response.topics == ['prices-v1', 'orders-v1']
    }
}
