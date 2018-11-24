package com.ranx;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class App {
	public static void main(String[] args) {
		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:application-context-rabbit.xml");
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		//发送消息
		template.convertAndSend("Hello World!");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//销毁容器
		ctx.destroy();
	}
}
