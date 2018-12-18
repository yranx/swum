package com.ranx.rpcmq;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


/**
 * @Description RPC服务端
 * @author ranx
 * @date 2018年11月14日 下午3:33:58
 *
 */
public class RPCMQServicer {
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
        if (n == 0) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] args) {
        System.out.println("服务端");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // 建立连接、通道、队列
        Connection connection = null;
        try {
            connection = factory.newConnection();
            final Channel channel = connection.createChannel();
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            channel.queuePurge(RPC_QUEUE_NAME); //这里清理了旧有数据
            channel.basicQos(1);
            // QueueingConsumer在Rabbitmq客户端3.x版本中广泛应用，但是在4.x版本开初就被标记为@Deprecated
            //		QueueingConsumer consumer = new QueueingConsumer(channel);
            System.out.println("RPCServer Waiting RPC request");
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                            .correlationId(properties.getCorrelationId()).build();
                    String response = "";
                    try {
                        String message = new String(body, "UTF-8");
                        int n = Integer.parseInt(message);
                        System.out.println("RPCServer fib(" + message + ")");
                        response += fib(n);
                    } catch (RuntimeException  e) {
                        System.out.println(" [.] " + e.toString());
                    }finally {
                        System.out.println("RPCServer result = " + response);
                        // 回送消息，响应
                        channel.basicPublish( "", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        // RabbitMq consumer worker thread notifies the RPC server owner thread
                        synchronized (this) {
                            this.notify();
                        }

                    }
                }
            };
            channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
            //等待并准备使用来自RPC Client的消息
            while (true) {
                synchronized (consumer) {
                    try {
                        consumer.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null)
                try {
                    connection.close();
                } catch (IOException _ignore) {}
        }
    }
}
