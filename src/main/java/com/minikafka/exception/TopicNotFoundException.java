package com.minikafka.exception;

/**
 * Thrown when an operation targets a topic that does not exist on the broker.
 */
public class TopicNotFoundException extends MiniKafkaException {
    private final String topic;

    public TopicNotFoundException(String topic) {
        super("Topic not found: " + topic);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
