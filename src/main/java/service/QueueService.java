package service;

import domain.Message;
import domain.Topic;
import exception.QueueException;

public interface QueueService {

    void addTopic(Topic topic) throws QueueException;

    boolean publishMessage(String topicName, Message message) throws QueueException;

    Message consumeMessage(String topicName, int partition) throws QueueException;

    Message consumeMessage(String topicName) throws QueueException;

}
