package main;

import util.ActiveMqUtil;

import javax.jms.*;
import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2021/5/26 9:39
 * description：
 */
public class Main {

    private static final String QUEUE_NAME = "queue1";

    public static void main(String[] args) {
    }

    /**
     * 测试有队列模型中有两个consumer时的消息消费情况
     */
    private static void test1(){
        new Thread(() -> {
            System.out.println("我是消费者" + Thread.currentThread().getName());
            XAConnection connection = null;
            Session session = null;
            MessageConsumer consumer = null;
            try {
                connection = ActiveMqUtil.getConnection();
                connection.start();
                // 创建Session，有两个参数，分别是是否开启事务、签收
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                // 创建目的地（可以是队列也可以是Topic）
                Queue queue = session.createQueue(QUEUE_NAME);
                // 创建消息消费者
                consumer = session.createConsumer(queue);
                while (true) {
                    TextMessage message = (TextMessage) consumer.receive();
                    if (message != null) {
                        System.out.println("消费者"+Thread.currentThread().getName()+"收到的消息是" + message.getText());
                    } else {
                        break;
                    }
                }
            } catch (JMSException e) {
                e.printStackTrace();
            } finally {
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
                if (session != null) {
                    try {
                        session.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "1").start();
        new Thread(() -> {
            System.out.println("我是消费者" + Thread.currentThread().getName());
            XAConnection connection = null;
            Session session = null;
            MessageConsumer consumer = null;
            try {
                connection = ActiveMqUtil.getConnection();
                connection.start();
                // 创建Session，有两个参数，分别是是否开启事务、签收
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                // 创建目的地（可以是队列也可以是Topic）
                Queue queue = session.createQueue(QUEUE_NAME);
                // 创建消息消费者
                consumer = session.createConsumer(queue);
                while (true) {
                    TextMessage message = (TextMessage) consumer.receive();
                    if (message != null) {
                        System.out.println("消费者"+Thread.currentThread().getName()+"收到的消息是" + message.getText());
                    } else {
                        break;
                    }
                }
            } catch (JMSException e) {
                e.printStackTrace();
            } finally {
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
                if (session != null) {
                    try {
                        session.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "2").start();
    }

    /**
     * 消费者使用receive接受消息
     */
    private static void testConsumerUseReceive(){
        XAConnection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        try {
            connection = ActiveMqUtil.getConnection();
            connection.start();
            // 创建Session，有两个参数，分别是是否开启事务、签收
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // 创建目的地（可以是队列也可以是Topic）
            Queue queue = session.createQueue(QUEUE_NAME);
            // 创建消息消费者
            consumer = session.createConsumer(queue);
            // 消费者应该一直等待接受消息
            while (true) {
                // 如果超过了1s还没有收到消息，就不在接受了
                TextMessage message = (TextMessage)consumer.receive(1000);
                if (message != null) {
                    System.out.println("消费者收到的消息是" + message.getText());
                } else {
                    break;
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 消费者使用listener接受消息
     */
    private static void testConsumerUseListener(){
        XAConnection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        try {
            connection = ActiveMqUtil.getConnection();
            connection.start();
            // 创建Session，有两个参数，分别是是否开启事务、签收
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // 创建目的地（可以是队列也可以是Topic）
            Queue queue = session.createQueue(QUEUE_NAME);
            // 创建消息消费者
            consumer = session.createConsumer(queue);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    if(message != null && message instanceof TextMessage){
                        TextMessage textMessage = (TextMessage) message;
                        try {
                            System.out.println("消费者收到的消息是" + textMessage.getText());
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            // 必要的步骤，防止消费者还没连接到mq，就被关闭了
            System.in.read();
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
