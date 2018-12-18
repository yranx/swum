package com.ranx.distributedLocks;

import java.util.Collections;

import redis.clients.jedis.Jedis;

/**
 * @Description 使用第三方开源组件Jedis实现Redis客户端，且只考虑Redis服务端单机部署的场景
 * @author ranx
 * @date 2018年12月18日 下午3:59:24
 * http://www.cnblogs.com/linjiqin/p/8003838.html
 * 原作者？？：https://wudashan.com/2017/10/23/Redis-Distributed-Lock-Implement/
 * 单机部署场景
 * (
 * Redis是多机部署的，那么可以尝试使用Redisson实现分布式锁，这是Redis官方提供的Java组件，
 * 链接https://wudashan.cn/2017/10/23/Redis-Distributed-Lock-Implement/#参考阅读
 * )
 */
public class DistributedLocksFour {
	private static final String LOCK_SUCCESS = "OK";
	private static final String SET_IF_NOT_EXIST = "NX";
	private static final String SET_WITH_EXPIRE_TIME = "PX";
	
	private static final Long RELEASE_SUCCESS = 1L;
	
	/**
	 * 获取分布式锁
	 * @param jedis
	 * @param lockKey 锁
	 * @param requestId 请求标识
	 * @param expireTime 超时时间
	 * @return 是否获取成功
	 */
	public static boolean getDistributedLock(Jedis jedis, String lockKey, String requestId, int expireTime) {
		/**
		 * lockKey:key锁
		 * requestId:value,可以使用UUID.randomUUID().toString()方法生成，用作校验是否是同一个线程
		 * nxxx：NX,SET IF NOT EXIST，即当key不存在时，我们进行set操作；若key已经存在，则不做任何操作；
		 * 可以保证如果已有key存在，则函数不会调用成功，也就是只有一个客户端能持有锁，满足互斥性
		 * expx：PX，表示要给这个key加一个过期的设置
		 * time：与第四个参数相呼应，代表key的过期时间。 锁的持有者后续发生崩溃而没有解锁，锁也会因为到了过期时间而自动解锁（即key被删除），不会发生死锁
		 *  操作结果：1. 当前没有锁（key不存在），那么就进行加锁操作，并对锁设置个有效期，同时value表示加锁的线程。
		 * 		2. 已有锁存在，不做任何操作。
		 */
		String result = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);
		if (LOCK_SUCCESS.equals(result)) {
			return true;
		}
		return false;
	}
	
	//错误示例
	/*DistributedLocksDemo.java类似操作
	 * 两条Redis命令，不具有原子性，如果程序在执行完setnx()之后突然崩溃，导致锁没有设置过期时间。那么将会发生死锁。
	 *  网上之所以有人这样实现，是因为低版本的jedis并不支持多参数的set()方法。
	 */
	public static void wrongGetLock1(Jedis jedis, String lockKey, String requestId, int expireTime) {
	    Long result = jedis.setnx(lockKey, requestId);
	    if (result == 1) {
	        // 若在这里程序突然崩溃，则无法设置过期时间，将发生死锁
	        jedis.expire(lockKey, expireTime);
	    }
	}
	
	/*
	 * DistributedLocks.java有点类似，这里流程简单些
	 * 实现思路：使用jedis.setnx()命令实现加锁，其中key是锁，value是锁的过期时间。
	 * 执行过程：1. 通过setnx()方法尝试加锁，如果当前锁不存在，返回加锁成功。
	 * 		2. 如果锁已经存在则获取锁的过期时间，和当前时间比较，如果锁已经过期，则设置新的过期时间，返回加锁成功。
	 *问题：1. 由于是客户端自己生成过期时间，所以需要强制要求分布式下每个客户端的时间必须同步。 （这个统一下DistributedLocksThree.java中有解决方案）
	 *2. 当锁过期的时候，如果多个客户端同时执行jedis.getSet()方法，那么虽然最终只有一个客户端可以加锁，但是这个客户端的锁的过期时间可能被其他客户端覆盖。（多线程不安全？？使用时应该加锁了？？）
	 *3. 锁不具备拥有者标识，即任何客户端都可以解锁。
	 */
	public static boolean wrongGetLock2(Jedis jedis, String lockKey, int expireTime) {

	    long expires = System.currentTimeMillis() + expireTime;
	    String expiresStr = String.valueOf(expires);

	    // 如果当前锁不存在，返回加锁成功
	    if (jedis.setnx(lockKey, expiresStr) == 1) {
	        return true;
	    }

	    // 如果锁存在，获取锁的过期时间
	    String currentValueStr = jedis.get(lockKey);
	    if (currentValueStr != null && Long.parseLong(currentValueStr) < System.currentTimeMillis()) {
	        // 锁已过期，获取上一个锁的过期时间，并设置现在锁的过期时间
	        String oldValueStr = jedis.getSet(lockKey, expiresStr);
	        if (oldValueStr != null && oldValueStr.equals(currentValueStr)) {
	            // 考虑多线程并发的情况，只有一个线程的设置值和当前值相同，它才有权利加锁
	            return true;
	        }
	    }   
	    // 其他情况，一律返回加锁失败
	    return false;

	}
	
	/**
	 *  释放分布式锁
	 * @param jedis
	 * @param lockKey
	 * @param requestId
	 * @return
	 */
	/*
	 *  为何使用Lua语言来实现呢？因为要确保上述操作是原子性的。
	 *  那么为什么执行eval()方法可以确保原子性，源于Redis的特性，下面是官网对eval命令的部分解释：
	 *  简单来说，就是在eval命令执行Lua代码的时候，Lua代码将被当成一个命令去执行，并且直到eval命令执行完成，Redis才会执行其他命令。
	 */
	 public static boolean releaseDistributedLock(Jedis jedis, String lockKey, String requestId) {
		 //Lua脚本代码, 获取锁对应的value值，检查是否与requestId相等，如果相等则删除锁（解锁）
		 String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
		 //将Lua代码传到jedis.eval()方法里，并使参数KEYS[1]赋值为lockKey，ARGV[1] 赋值为requestId。
		 //eval()方法是将Lua代码交给Redis服务端执行。
         Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));

         if (RELEASE_SUCCESS.equals(result)) {
             return true;
         }
         return false;
	 }
	
	//错误示例
	 /*
	  * 不先判断锁的拥有者而直接解锁的方式，会导致任何客户端都可以随时进行解锁，即使这把锁不是它的。
	  */
	public static void wrongReleaseLock1(Jedis jedis, String lockKey) {
		    jedis.del(lockKey);
	}
	
	/*
	 * 分成两条命令不具有原子性
	 * 问题在于如果调用jedis.del()方法的时候，这把锁已经不属于当前客户端的时候会解除他人加的锁。
	 * 那么是否真的有这种场景？答案是肯定的，比如客户端A加锁，一段时间之后客户端A解锁，在执行jedis.del()之前，锁突然过期了，
	 * 此时客户端B尝试加锁成功，然后客户端A再执行del()方法，则将客户端B的锁给解除了。
	 */
	public static void wrongReleaseLock2(Jedis jedis, String lockKey, String requestId) {        
	    // 判断加锁与解锁是不是同一个客户端
	    if (requestId.equals(jedis.get(lockKey))) {
	        // 若在此时，这把锁突然不是这个客户端的，则会误解锁
	        jedis.del(lockKey);
	    }
	}
}
