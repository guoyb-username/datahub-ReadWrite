package com.gyb.datahub;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DatahubReadConfig {
    @Value("${datahub.clients.default.accessId}")
    private String accessId;
    @Value("${datahub.clients.default.accessKey}")
    private String accessKey;
    @Value("${datahub.clients.default.endpoint}")
    private String endpoint;

    @Value("${datahub.topics.project}")
    private String projectName;
    @Value("${datahub.topics.topic1}")
    private String topicName1;
    @Value("${datahub.topics.topic2}")
    private String topicName2;
    @Value("${datahub.topics.topic3}")
    private String topicName3;

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTopicName1() {
        return topicName1;
    }

    public void setTopicName1(String topicName1) {
        this.topicName1 = topicName1;
    }

    public String getTopicName2() {
        return topicName2;
    }

    public void setTopicName2(String topicName2) {
        this.topicName2 = topicName2;
    }

    public String getTopicName3() {
        return topicName3;
    }

    public void setTopicName3(String topicName3) {
        this.topicName3 = topicName3;
    }
}
