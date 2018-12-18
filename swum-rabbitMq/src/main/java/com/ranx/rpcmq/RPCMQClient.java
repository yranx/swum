package com.ranx.rpcmq;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 使用RabbitMQ搭建一个RPC(远程过程调用)系统，一个客户端和一个可扩展的RPC服务器:
 一个客户端发送一个请求信息和一个响应信息的服务器回复

 AMQP协议一共预定义了14个属性，常用的：
 deliveryMode：有2个值，一个是持久，另一个表示短暂
 contentType：内容类型：用来描述编码的MIME类型。（如：application/json）
 replyTo：回调队列的名字
 correlationid：RPC响应请求的相关应用

 Correlation Id
 在队列上接收到一个响应，但并不清楚响应属于哪一个，就需要为每个请求设置唯一值:CorrelationId属性，
 稍后在回调队列中接收消息的时候，会看到这个属性，如果看到一个未知的CorrelationId，我们就可以安全地忽略信息-它不属于我们的请求。
 为什么我们应该忽略未知的消息在回调队列中，而不是失败的错误？这是由于服务器端的一个竞争条件的可能性。
 比如还未发送了一个确认信息给请求，但是此时RPC服务器挂了。如果这种情况发生，将再次重启RPC服务器处理请求。这就是为什么在客户端必须处理重复的反应。
 */
/**
 * RPC原理
 * 客户端发送消息到MQ队列，服务端从队列取得消息进行处理，再将结果回传到客户端
 */
/**
 * @Description 客户端
 * @author ranx
 * @date 2018年11月14日 下午4:05:26
 *
 */
public class RPCMQClient {
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private Connection connection;
    private Channel channel;

    public RPCMQClient() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // 建立连接、通道
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public String call(String message) throws  IOException, InterruptedException {
        // 生成一个唯一的correlationid
        final String corrID = UUID.randomUUID().toString();
        // 声明一个唯一的“回调”队列的答复
        String replyQueueName = channel.queueDeclare().getQueue();
        /**
         * 回调队列: 发送一个回调队列地址请求（发送2个属性，replyTo 和correlationId），以得到响应
         * AMQP协议一共预定义了14个属性，常用的： correlationId : RPC响应请求的相关编号 replyTo：回调队列的名称
         * contentType：内容类型：用来描述编码的MIME类型。（如：application/json）
         * deliveryMode：传递模型，配置一个消息是否持久化，持久/临时
         */
        AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                .correlationId(corrID).replyTo(replyQueueName)
                .build();
//		channel.basicPublish("", RPC_QUEUE_NAME, null, message.getBytes("UTF-8"));
        channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        //basic.consume指的是channel在 某个队列上注册消费者,那在这个队列有消息来了之后,就会把消息转发到给此channel处理
        String ctag = channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {

                if (properties.getCorrelationId().equals(corrID)) {
                    response.offer(new String(body, "UTF-8"));
//					System.out.println("RPCClient  Got '" + response + "'");
                }
            }
        });
        String result = response.take();
        channel.basicCancel(ctag);
        return result;

    }

    public static void main(String[] args) throws Exception {
        System.out.println("客户端");
        RPCMQClient rpcMQC = null;
        String response = null;

        try {
            rpcMQC = new RPCMQClient();
            // 先发请求操作的消息
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.print(" RPCClient Requesting fib( " + i_str + " ) ========");
                response = rpcMQC.call(i_str);
                System.out.println(" RPCClient Got ' " + response + " '");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭通道和连接
            if (rpcMQC != null) {
                rpcMQC.connection.close();
            }
        }
    }
}
