package service;

import domain.Message;
import domain.Topic;
import exception.QueueException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueueServiceTest {

    Topic topic1, topic2;
    Message message1, message2, message3;
    private QueueService service = new QueueServiceImpl();

    @BeforeEach
    void setUp() throws QueueException {
        topic1 = new Topic("Order", 5);
        topic2 = new Topic("Notification", 2);
        message1 = new Message("1122", "Your order has been placed");
        message2 = new Message("3344", "Your order has been dispatched");
        message3 = new Message("7650", "Your order has been delivered");
        service.addTopic(topic1);
        service.addTopic(topic2);
        service.publishMessage(topic1.getName(), message1);
        service.publishMessage(topic1.getName(), message2);
        service.publishMessage(topic1.getName(), message3);
        service.publishMessage(topic2.getName(), message3);
        service.publishMessage(topic2.getName(), message2);
    }

    @Test
    void test_Add_topic() throws QueueException {
        Topic topic = new Topic("query", 3);
        service.addTopic(topic);
    }

    @Test
    void test_Add_topic_already_exists_error() {
        Topic topic = new Topic("Order", 20);
        Assertions.assertThrows(QueueException.class, () ->
                service.addTopic(topic));
    }

    @Test
    void test_Add_topic_already_exceed_max_min_partition() {
        Topic topic = new Topic("test", 20000);
        Assertions.assertThrows(QueueException.class, () ->
                service.addTopic(topic));

        Topic topic1 = new Topic("test", 0);
        Assertions.assertThrows(QueueException.class, () ->
                service.addTopic(topic1));
    }

    @Test
    void test_add_publish_message() throws QueueException {
        Topic topic = new Topic("settlement", 5);
        service.addTopic(topic);
        Message message = new Message("112565", "Order has been processed");
        Message message2 = new Message("1425", "Order has been processed");
        boolean b = service.publishMessage(topic.getName(), message);
        boolean b1 = service.publishMessage(topic.getName(), message2);
        Assertions.assertTrue(b);
        Assertions.assertTrue(b1);
    }

    @Test
    void test_consume_message() throws QueueException {
        Message responseMessage = service.consumeMessage(topic1.getName());
        Message responseMessage2 = service.consumeMessage(topic2.getName());
        Assertions.assertNotNull(responseMessage);
        Assertions.assertNotNull(responseMessage2);
    }

    @Test
    void test_consume_message_from_partition() throws QueueException {
        service.consumeMessage(topic1.getName(), 0);
        service.consumeMessage(topic2.getName(), 0);
    }


}