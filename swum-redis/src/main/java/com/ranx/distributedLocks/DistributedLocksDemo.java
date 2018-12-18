package com.ranx.distributedLocks;

import java.util.List;

import com.ranx.jedis.JedisPoolUtil;
import com.ranx.utils.UUIDUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * @Description redis实现分布式锁
 * @author ranx
 * @date 2018年12月18日 上午11:31:10
 * https://www.cnblogs.com/liuyang0/p/6744076.html
 */
/**
 * 思路 
 * 1.获取锁的时候，使用setnx加锁，并使用expire命令为锁添加一个超时时间，超过该时间则自动释放锁，
 * 锁的value值为一个随机生成的UUID，通过此在释放锁的时候进行判断。
 * 设置一个获取的超时时间，若超过这个时间则放弃获取锁。
 * 2.释放锁的时候，通过UUID判断是不是该锁，若是该锁，则执行delete进行锁释放。
 */
/**
SETNX key val		当且仅当key不存在时，set一个key为val的字符串，返回1；若key存在，则什么都不做，返回0。
expire key timeout  为key设置一个超时时间，单位为second，超过这个时间锁会自动释放，避免死锁。
delete key			删除key
 */
public class DistributedLocksDemo {
//	private final JedisPool jedisPool;
	
	/**
	 *  加锁 （实际是在redis里面加kv）
	 * @param locaName 锁的key
	 * @param acquireTimeout 获取超时时间，单位毫秒， 即：加锁时间内执行完操作，如果未完成会有并发现象 
	 * @param timeout 锁的超时时间
	 * @return 锁标识
	 */
	public static String lockWithTimeout(String locaName, long acquireTimeout, long timeout) {
		Jedis jedis = null;
		String retIdentifier = null;
		
		try {
			//获取连接
			jedis = JedisPoolUtil.getJedisPoolInstance().getResource();
			//随机生成key
			String identifier = UUIDUtils.getUUID();
			//锁名
			String lockKey = "lock:" + locaName;
			//超时时间
			int lockExpire = (int)(timeout/ 1000);
			// 获取锁的超时时间，超过这个时间则放弃获取锁
            long end = System.currentTimeMillis() + acquireTimeout;
			while (System.currentTimeMillis()  < end) {
				/**
				 * 加锁问题：由于setnx和expire两个操作非原子性，如果setnx成功了，
				 * expire时，该应用发生故障，甚至网络断开导致expire不成功，则会产生一个死锁
				 */
				if (jedis.setnx(lockKey, identifier) == 1) { //key不存在
					jedis.expire(lockKey, lockExpire);
					retIdentifier = identifier;
					return identifier; //返回value,用于释放锁的时间确认
				}
				
				// 返回-1代表key没有设置超时时间，为key设置一个超时时间
				if (jedis.ttl(lockKey) == -1) {
					jedis.expire(lockKey, lockExpire);
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
		return retIdentifier;	
	}
	
	/**
	 * 释放锁
	 * @param lockName 锁的key
	 * @param identifier 释放锁的标识
	 * @return
	 */
	/*
	 * 用delete解锁 问题重重
	 */
	public static boolean releaseLock(String lockName, String identifier) {
		Jedis jedis = null;
		String lockKey = "lock:" + lockName;
		boolean retFlag = false;
		try {
			jedis = JedisPoolUtil.getJedisPoolInstance().getResource();
			while (true) {
				//监视lock,准备开始事物
				jedis.watch(lockKey);
				//通过前面返回的value值判断是不是锁，若是该锁，则删除，释放锁
				if (identifier.equals(jedis.get(lockKey))) {
					Transaction transaction = jedis.multi();
					transaction.del(lockKey);
					List<Object> results = transaction.exec();
					if (results == null) {
						continue;
					}
					retFlag = true;
				}
				jedis.unwatch();
				break;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
		return retFlag;
	}
	
	
	static class ThreadA implements Runnable{
		private int n = 500;
		/**
		 * 线程开多了，死锁？？
		 */
		@Override
		public void run() {
			while (true) {
					if (n < 1) {
						String resource = UUIDUtils.getUUID();
						// 返回锁的value值，供释放锁时候进行判断
				        String indentifier = DistributedLocksDemo.lockWithTimeout(resource, 5000, 1000);
				        System.out.println(Thread.currentThread().getName() + "获得了锁");
				        System.out.println(--n);
				        DistributedLocksDemo.releaseLock(resource, indentifier);
					}	
				}
		}
	}
	
	public static void main(String[] args) {
		//使用50个线程模拟秒杀一个商品，使用--运算符来实现商品减少，从结果有序性就可以看出是否为加锁状态。
		for (int i=0; i<1000; i++) {
			ThreadA threadA = new ThreadA();
			Thread t = new Thread(threadA);
			t.start();	

		}
	}
}
