package com.ranx.consumer;

/**
 * @Description 消费者                                                                                                                                                                                                                                                                                                                                                              
 * @author ranx
 * @date 2018年11月23日 上午11:57:59
 *
 */
public class FanoutConsumer {

	//具体执行业务的方法
	public void listen(String message) {
		System.out.println("Counsumer:" + message);
	}
}
