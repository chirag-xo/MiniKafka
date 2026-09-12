package com.minikafka.exception;

/**
 * Thrown when attempting to create a topic that is already registered on the broker.
 */
public class TopicAlreadyExistsException extends MiniKafkaException {
    private final String topic;

    public TopicAlreadyExistsException(String topic) {
        super("Topic already exists: " + topic);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
