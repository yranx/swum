package com.ranx;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

/**
 * @Description
 * @author ranx
 * @date 2018年11月13日 下午3:45:00
 *
 */
public class RabbitMQDemo {

    private final static String QUEUE_NAME = "rabbitMq";
    private final static String TASK_QUEUE_NAME = "task_queue";
    private static final String EXCHANGE_NAME = "logs";
    private static final String ROUTING_EXCHANGE_NAME ="direct_logs";
    private static final String TOPICS_EXCHANGE_NAME ="topics_logs";
    // 路由关键字
    private static final String[] routingKeys = new String[]{"info" ,"warning", "error"};
    /**
     * 发送信息到队列然后队列把信息在发送到消费者，
     */
    // 消息生成者
    public static void producer(Channel channel, int i) throws Exception {
        /**
         * 声明一个队列 queueDeclare(queue队列名
         * ,durable是否持久化
         * ,exclusive是否独占队列：创建者可以用的私有队列，断开后自动删除
         * ,autoDelete 当所有消费者客户端连接断开时是否自动删除队列
         * ,arguments 队列的其他参数)
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        // 消息
        String message = "RabbitMQ message";
        StringBuffer sbf = new StringBuffer();
        sbf.append(i).append("=====").append(message);
        /**
         * 发送消息到队列中 basicPublish(exchange 交换机名称 , routingKey 对列映射的路由key , props 消息的其他属性
         * , body 发送信息的主体);
         */
        channel.basicPublish("", QUEUE_NAME, null, sbf.toString().getBytes("UTF-8"));
        System.out.println("Producer Send + '" + sbf + "'");

    }

    // 消费者
    public static void customer(Channel channel) throws Exception {
        // 声明要关注的队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("Customer Waiting Received messages");
        // DefaultConsumer类实现了Consumer接口，通过传入一个通道，
        // 告诉服务器我们需要那个通道的消息，如果通道中有消息，就会执行回调函数handleDelivery
        Consumer consumer = new DefaultConsumer(channel) {
            /**
             * envelope主要存放生产者相关信息（比如交换机、路由key等） body是消息实体
             */
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Customer Received '" + message + "'");
            }
        };
        // 自动回复队列应答 -- RabbitMQ中的消息确认机制
        /**
          basicConsume的第二个参数autoAck: 应答模式，
         true：自动应答，即消费者获取到消息，该消息就会从队列中删除掉，
         false：手动应答，当从队列中取出消息后，需要程序员手动调用方法应答，
                如果没有应答，该消息还会再放进队列中，就会出现该消息一直没有被消费掉的现象
         */
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }


    /**
     *如果积累了大量工作，就需要更多的工作者来处理，这里就要采用分布机制了。
     */
    // 生产者Task
    public static void proTask(Channel channel) throws Exception {
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        // 分发信息
        for (int i = 0; i < 10; i++) {
            String message = "ProTask rabbitmq ===== " + i;
            /**
             * MessageProperties.PERSISTENT_TEXT_PLAIN : 标识我们的信息为持久化的
             */
            channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println("ProTask send '" + message + "'");
        }
    }

    //worker
    public static void cusWorker(Channel channel, int i) throws Exception {
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println("Worker **" + i + "** Waiting for message");
        //每次从队列获取的数量
        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Worker **" + i + "** Received '" + message + "'");
                try {
                    //throw new Exception();
                    Thread.sleep(1*1000);
                } catch (Exception e) {
                    channel.abort();
                } finally {
                    System.out.println(envelope.getDeliveryTag() + " ··· Producer's proMes DeliveryTag");
                    System.out.println("Worker **" + i + "** done");
                    /**
                     *  basicAck(long deliveryTag, boolean multiple)
                     *  deliveryTag:该消息的index,
                     *  multiple：是否批量.true:将一次性ack所有小于deliveryTag的消息。
                     */
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        /**
         * autoAck是否自动回复，如果为true的话，每次生产者只要发送信息就会从内存中删除，那么如果消费者程序异常退出，那么就无法获取数据，
         * 为了避免这样的情况，采用手动回复，每当消费者收到并处理信息然后在通知生成者，最后从队列中删除这条信息。
         * 如果消费者异常退出，如果还有其他消费者，那么就会把队列中的消息发送给其他消费者，如果没有，等消费者启动时候再次发送。
         */
        boolean autoAck = false;
        //消息消费完成确认
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);

    }


    /**
     * rabbitMQ其实真正的思想是生产者不发送任何信息到队列，甚至不知道信息将发送到哪个队列(队列并没有指定名称)。
     * 生产者只能发送信息到交换机，交换机接收到生产者的信息，然后按照规则把它推送到特定的队列
     */
    /**
     * 发布、订阅
     * 采用了广播的模式进行消息的发送
     */
    //生产者
    public static void emitLogs(Channel channel) throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME,  "fanout"); //fanout表示分发，所有的消费者得到同样的队列消息
        // 分发信息
        for (int i = 0; i < 5; i++) {
            String message = "ProEmitLogs ===== " + i;
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            System.out.println("ProEmitLogs send '" + message + "'");
        }
    }
    //消费者
    public static void receiveLogs(Channel channel) throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME,  "fanout");
        //获取一个新的空的随机名称的队列 的名称
        String queueName = channel.queueDeclare().getQueue();
        //对队列进行绑定
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println("ReceiveLogs Waiting for message");
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "utf-8");
                System.out.println("ReceiveLogs Received '" + message + "'");
            }
        };
        //断开用户的队列，自动进行删除
        channel.basicConsume(queueName, true, consumer);

    }

    /**
     * Routing
     * 采用路由的方式对不同的消息进行过滤
     */
    //发送端
    public static void routingSendDirect(Channel channel) throws Exception {
        //声明交换机
        channel.exchangeDeclare(ROUTING_EXCHANGE_NAME, "direct");
        //发送信息
        for (String routingKey : routingKeys) {
            String message = "RoutingSendDirect Send the message level:" + routingKey;
            channel.basicPublish(ROUTING_EXCHANGE_NAME, routingKey, null, message.getBytes());
            System.out.println(message);
        }
    }
    //消费者
    public static void receivedLogsDirect(Channel channel) throws Exception {
        //声明交换器
        channel.exchangeDeclare(ROUTING_EXCHANGE_NAME, "direct");
        //获取匿名队列名称
        String queueName=channel.queueDeclare().getQueue();

        //根据路由关键字进行绑定
        for (String routingKey : routingKeys) {
            channel.queueBind(queueName, ROUTING_EXCHANGE_NAME, routingKey);
        }
        System.out.println("ReceiveLogsDirect Waiting for messages");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("ReceiveLogsDirect Received '" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    /**
     * Topics
     * 模糊匹配     * ：可以替代一个词   #：可以替代0或者更多的词
     */
    //发送端
    public static void topicSend(Channel channel) throws Exception {
        //声明一个匹配模式的交换机
        channel.exchangeDeclare(TOPICS_EXCHANGE_NAME, "topic");
        //待发送的消息
        String[] routingKeys=new String[]{
                "quick.orange.rabbit",
                "lazy.orange.elephant",
                "quick.orange.fox",
                "lazy.brown.fox",
                "quick.brown.fox",
                "quick.orange.male.rabbit",
                "lazy.orange.male.rabbit"
        };
        //发送消息
        for(String severity :routingKeys){
            String message = "From "+severity+" routingKey' s message!";
            channel.basicPublish(TOPICS_EXCHANGE_NAME, severity, null, message.getBytes());
            System.out.println("TopicSend Sent '" + severity + "' : '" + message + "'");
        }
    }
    //消费者
    public static void receiveLogsTopics(Channel channel) throws Exception {
        //声明一个匹配模式的交换机
        channel.exchangeDeclare(TOPICS_EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();
        //路由关键字
        String[] routingKeys = new String[]{"*.orange.*"};
//        String[] routingKeys = new String[]{"*.*.rabbit", "lazy.#"}; //不同的消费者用不同的路由
        //绑定路由
        for (String routingKey : routingKeys) {
            channel.queueBind(queueName, TOPICS_EXCHANGE_NAME, routingKey);
        }
        System.out.println("ReceiveLogsTopic Waiting for messages");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("ReceiveLogsTopic Received '" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    public static void main(String[] args) throws Exception {
        // 创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        // 设置RabbitMQ地址
        factory.setHost("localhost");
        // 创建一个连接
        Connection connnection = factory.newConnection();
        // 创建一个通道
        Channel channel = connnection.createChannel();

        try {
            //Test1
            //模拟生成多条消息
//			for (int i = 1; i < 6; i++) {
//				producer(channel, i);
//				Thread.sleep(3 * 1000);
//			}
//			customer(channel);
//			Thread.sleep(10 * 1000);

            //Test2
//			proTask(channel);
//			//模拟两个worker处理消息
//			for (int i=1; i<3; i++) {
//				cusWorker(channel, i);
//				Thread.sleep(1 * 1000);
//			}
//			Thread.sleep(30 * 1000);

            //Test3
//			emitLogs(channel);
//			receiveLogs(channel);

            //Test4
//			routingSendDirect(channel);
//			receivedLogsDirect(channel); //需要启动两个控制台运行该行才能看到效果

            //Test5
            topicSend(channel);
            receiveLogsTopics(channel);

//			Thread.sleep(30 * 1000);
        } finally {
            // 关闭通道和连接
            channel.close();
            connnection.close();
            System.out.println("END!");
        }
    }
}

