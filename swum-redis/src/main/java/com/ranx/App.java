package com.ranx;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ranx.service.RedisObjectService;

/**
 * Hello world!
 *
 */
public class App {
	@Autowired
	private static RedisObjectService redisObjectServices;
    public static void main( String[] args ){
        System.out.println( "Hello World!" );
    	ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-context-redis.xml");
    	redisObjectServices = (RedisObjectService) context.getBean("redisObjectService");
    	redisObjectServices.setValue("redis", "deepLearning");
    	System.out.println(redisObjectServices.getValue("redis"));
		System.out.println(redisObjectServices.getValue("redis"));
		System.out.println(redisObjectServices.getValue("redis"));
		System.out.println(redisObjectServices.getValue("redis"));

    }
}
