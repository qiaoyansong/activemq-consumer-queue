package util;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2021/5/26 9:33
 * description：
 * 基本与mybatis获取Session的方式一致
 */
public class ActiveMqUtil {

    private static final String ACTIVE_MQ_URL = "tcp://192.168.40.128:61616";

    private ActiveMqUtil() {

    }

    public static Connection getConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ACTIVE_MQ_URL);
        Connection connection = factory.createConnection();
        return connection;
    }
}
