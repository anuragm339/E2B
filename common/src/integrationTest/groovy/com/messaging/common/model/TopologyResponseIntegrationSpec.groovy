package com.messaging.common.model

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

@MicronautTest(startApplication = false)
class TopologyResponseIntegrationSpec extends Specification {

    private final ObjectMapper objectMapper = new ObjectMapper()

    def "topology response round-trips through Jackson with node role and topic lists"() {
        given:
        def json = '''
        {
          "nodeId": "broker-root",
          "role": "ROOT",
          "requestToFollow": ["http://l2-a", "http://l2-b"],
          "cloudDataUrl": "http://cloud-primary",
          "cloudDataUrlFallback": "http://cloud-secondary",
          "topologyVersion": "v9",
          "topics": ["prices-v1", "orders-v1"]
        }
        '''

        when:
        def response = objectMapper.readValue(json, TopologyResponse)
        def written = objectMapper.writeValueAsString(response)

        then:
        response.nodeId == 'broker-root'
        response.role == TopologyResponse.NodeRole.ROOT
        response.requestToFollow == ['http://l2-a', 'http://l2-b']
        response.topics == ['prices-v1', 'orders-v1']
        written.contains('"role":"ROOT"')
    }
}
