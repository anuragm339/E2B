package com.messaging.common.model;

import java.util.List;

/**
 * Response from cloud registry for topology query
 */
public class TopologyResponse {
    private String nodeId;
    private NodeRole role;
    private List<String> requestToFollow;  // Ordered list of parent URLs to try
    private String cloudDataUrl;           // For root broker only
    private String cloudDataUrlFallback;   // For root broker only
    private String topologyVersion;
    private List<String> topics;

    public TopologyResponse() {
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public NodeRole getRole() {
        return role;
    }

    public void setRole(NodeRole role) {
        this.role = role;
    }

    public List<String> getRequestToFollow() {
        return requestToFollow;
    }

    public void setRequestToFollow(List<String> requestToFollow) {
        this.requestToFollow = requestToFollow;
    }

    public String getCloudDataUrl() {
        return cloudDataUrl;
    }

    public void setCloudDataUrl(String cloudDataUrl) {
        this.cloudDataUrl = cloudDataUrl;
    }

    public String getCloudDataUrlFallback() {
        return cloudDataUrlFallback;
    }

    public void setCloudDataUrlFallback(String cloudDataUrlFallback) {
        this.cloudDataUrlFallback = cloudDataUrlFallback;
    }

    public String getTopologyVersion() {
        return topologyVersion;
    }

    public void setTopologyVersion(String topologyVersion) {
        this.topologyVersion = topologyVersion;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public enum NodeRole {
        ROOT,
        L2,
        L3,
        POS,
        LOCAL
    }
}
