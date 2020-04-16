package service;

import dao.TopicDao;
import domain.Message;
import domain.Topic;
import exception.QueueException;

import java.util.Map;
import java.util.Objects;
import java.util.Queue;

public class QueueServiceImpl implements QueueService {

    private TopicDao topicDao;

    public QueueServiceImpl() {
        this.topicDao = new TopicDao();
    }

    @Override
    public void addTopic(Topic topic) throws QueueException {
        topicDao.addTopic(topic);
    }

    @Override
    public boolean publishMessage(String topicName, Message message) throws QueueException {
         return topicDao.publishMessage(topicName, message);
    }

    @Override
    public Message consumeMessage(String topicName, int partition) throws QueueException {
        Queue<Message> partitionData = topicDao.getPartition(topicName, partition);
        if (Objects.nonNull(partitionData.peek()))
            return partitionData.poll();
        else
            return null;
    }

    @Override
    public Message consumeMessage(String topicName) throws QueueException {
        Map<Integer, Queue<Message>> topicData = topicDao.getTopicData(topicName);
        for (Map.Entry<Integer, Queue<Message>> x : topicData.entrySet()) {
            if (x.getValue().size() > 0) {
                return x.getValue().poll();
            }
        }
        return null;
    }
}
