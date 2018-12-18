package com.ranx;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * @Description
 * @author ranx
 * @date 2018年11月28日 上午10:46:35
 *
 */
public class RabbitMqDemo2 {

	private final static String HEADERS_EXCHANGE_NAME = "hearders";
	private final static String EXCHANGE_DEAD_NAME = "exchange.dead";
	private final static String QUEUE_DEAD_NAME = "queue_dead";
	private final static String EXCHANGE_NAME = "exchange.fanout";
	private final static String QUEUE_NAME = "queue_name";
	/**
	 * 首部交换机:交换机通过Headers头部来将消息映射到队列的，有点像HTTP的Headers，
	 * Hash结构中要求携带一个键“x-match”，这个键的Value可以是any(仅匹配一个键)或者all(全部匹配)，{"x-match":"any"}
	 * 优势:匹配的规则不被限定为字符串(string)而是Object类型。
	 * any:只要在发布消息时携带的有一对键值对headers满足队列定义的多个参数arguments的其中一个就能匹配上，
	 * 注意这里是键值对的完全匹配，只匹配到键了，值却不一样是不行的； all：在发布消息时携带的所有Entry必须和绑定在队列上的所有Entry完全匹配
	 * 
	 * https://blog.csdn.net/vbirdbest/article/details/78638988
	 * 博客的rabbitMQ web查看也是值得学习的
	 */
	public static void HeaderPro(Channel channel) throws Exception {
		channel.exchangeDeclare(HEADERS_EXCHANGE_NAME, "headers");
		Map<String, Object> heardersMap = new HashMap<String, Object>();
		heardersMap.put("api", "login");
		heardersMap.put("version", 1.0);
		heardersMap.put("radom", UUID.randomUUID().toString());
		AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties().builder().headers(heardersMap);

		String message = "Hello RabbitMQ!";
		channel.basicPublish(HEADERS_EXCHANGE_NAME, "", properties.build(), message.getBytes("UTF-8"));
	}

	public static void HeaderCus(Channel channel) throws Exception {
		channel.exchangeDeclare(HEADERS_EXCHANGE_NAME, "headers");
		String queueName = channel.queueDeclare().getQueue();
		Map<String, Object> arguments = new HashMap<String, Object>();
		arguments.put("x-match", "any");
		arguments.put("api", "login");
		arguments.put("version", 1.0);
		arguments.put("dataType", "json");
		// 队列绑定时需要指定参数,注意虽然不需要路由键但仍旧不能写成null，需要写成空字符串""
		channel.queueBind(queueName, HEADERS_EXCHANGE_NAME, "", arguments);
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				System.out.println(message);
			}
		};

		channel.basicConsume(queueName, true, consumer);
		Thread.sleep(10 * 1000);
	}

	/**
	 * Dead letter exchange(死亡交换机) 和 Dead letter routing key(死亡路由键)
	 * 当队列中的消息过期，或者达到最大长度而被删除，或者达到最大空间时而被删除时，
	 * 可以将这些被删除的信息推送到其他交换机中，让其他消费者订阅这些被删除的消息，处理这些消息
	 * 
	 * https://blog.csdn.net/vbirdbest/article/details/78670550
	 */
	public static void deadBasicPublish(Channel channel) throws IOException, TimeoutException, InterruptedException {
		// 声明一个接收被删除的消息的交换机和队列
		channel.exchangeDeclare(EXCHANGE_DEAD_NAME, "direct");
		channel.queueDeclare(QUEUE_DEAD_NAME, false, false, false, null);
		channel.queueBind(QUEUE_DEAD_NAME, EXCHANGE_DEAD_NAME, "routingkey.dead");

		// 广播队列
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

		Map<String, Object> arguments = new HashMap<String, Object>();
		// 统一设置队列中的所有消息的过期时间
		arguments.put("x-message-ttl", 15000);
		// 设置队列的最新的N条消息，如果超过N条，前面的消息将从队列中移除掉
		arguments.put("x-max-length", 4);
		// 设置队列的内容的最大空间，超过该阈值就删除之前的消息
		arguments.put("x-max-length-bytes", 1024);
		// 设置超过多少毫秒没有消费者来访问队列，就删除队列的时间
		arguments.put("x-expires", 30000);
		// 将删除的消息推送到指定的交换机，一般x-dead-letter-exchange和x-dead-letter-routing-key需要同时设置
		arguments.put("x-dead-letter-exchange", "exchange.dead");
        // 将删除的消息推送到指定的交换机对应的路由键
		arguments.put("x-dead-letter-routing-key", "routingkey.dead");
		 // 设置消息的优先级，优先级大的优先被消费
        arguments.put("x-max-priority", 10);
		channel.queueDeclare(QUEUE_NAME, true, false, false, arguments);
		channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");

		String message = "Hello RabbitMQ: ";
		for (int i = 1; i <= 5; i++) {
			// expiration: 设置单条消息的过期时间
            AMQP.BasicProperties.Builder properties = 
            		new AMQP.BasicProperties().builder().priority(i).expiration( i * 1000 + "");
			channel.basicPublish(EXCHANGE_NAME, "", properties.build(), (message + i).getBytes("UTF-8"));
		}
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
//			HeaderPro(channel);
//			HeaderCus(channel);
			deadBasicPublish(channel);
		} finally {
			// 关闭通道和连接
			if (channel != null) {
				channel.close();
			}
			if (connnection != null) {
				connnection.close();
			}
			System.out.println("END!");
		}
	}
}