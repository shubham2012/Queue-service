package dao;

import domain.Message;
import domain.Topic;
import exception.QueueException;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@ToString
public class TopicDao {

    private static int MAX_PARTITION = 100;
    private static int MIN_PARTITION = 1;

    private Map<String, Map<Integer, Queue<Message>>> topicsData;

    private Map<String, Topic> topics;

    public TopicDao() {
        this.topicsData = new HashMap<>();
        this.topics = new ConcurrentHashMap<>();
    }

    public Map<String, Map<Integer, Queue<Message>>> getAllTopicsData() {
        return topicsData;
    }

    public Map<Integer, Queue<Message>> getTopicData(String topic) throws QueueException {
        if (!topicsData.containsKey(topic)) {
            throw new QueueException("Topic with name", topic, "doesn't exists.");
        }
        return topicsData.get(topic);
    }

    public Queue<Message> getPartition(String topic, Integer partition) throws QueueException {
        if (!getTopicData(topic).containsKey(partition)) {
            throw new QueueException("Partition with key", String.valueOf(partition), "doesn't exists.");
        }
        return getTopicData(topic).get(partition);
    }

    public void addTopic(Topic topic) throws QueueException {
        if (topicsData.containsKey(topic.getName())) {
            throw new QueueException("Topic with name", topic.getName(), "already exists.");
        }
        if(StringUtils.isEmpty(topic.getName())){
            throw new QueueException("Please provide a valid topic name");
        }
        if (topic.getPartitions() > MAX_PARTITION) {
            throw new QueueException("Max Partition allowed are", String.valueOf(MAX_PARTITION));
        }
        if (topic.getPartitions() < MIN_PARTITION) {
            throw new QueueException("Min Partition allowed are", String.valueOf(MIN_PARTITION));
        }
        topics.put(topic.getName(), topic);
        Map<Integer, Queue<Message>> partitions = new ConcurrentHashMap<>();
        int size = topic.getPartitions();
        for (int i = 0; i < size; i++) {
            partitions.put(i, new LinkedBlockingQueue<>());
        }
        topicsData.put(topic.getName(), partitions);
    }

    public boolean publishMessage(String topicName, Message message) throws QueueException {
        if (!topics.containsKey(topicName)) {
            throw new QueueException("Topic with name", topicName, "doesn't exists.");
        }
        if (StringUtils.isEmpty(message.getKey())) {
            throw new QueueException("Key can not be empty");
        }
        String key = message.getKey();
        Topic topic = topics.get(topicName);
        int partition = Math.abs(key.hashCode()) % topic.getPartitions();
        return topicsData.get(topicName).get(partition).offer(message);
    }

}
