package com.ranx.distributedLocks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * @Description redis实现分布式锁(三)
 * @author ranx
 * @date 2018年12月18日 下午3:06:43
  *    有点问题
 */
/**
 * Redis实现分布式锁的原理： 1.通过setnx(lock_timeout)实现，如果设置了锁返回1， 已经有值没有设置成功返回0
 * 2.死锁问题：通过时间来判断是否过期，如果已经过期，获取到过期时间get(lockKey)，然后getset(lock_timeout)判断是否和get相同，相同则证明已经加锁成功，
 * 因为可能导致多线程同时执行getset(lock_timeout)方法，这可能导致多线程都只需getset后，对于判断加锁成功的线程，
 * 再加expire(lockKey, LOCK_TIMEOUT,
 * TimeUnit.MILLISECONDS)过期时间，防止多个线程同时叠加时间，导致锁时效时间翻倍
 * 3.针对集群服务器时间不一致问题，可以调用redis的time()获取当前时间
 *https://www.jb51.net/article/118312.htm
 */
public class DistributedLocksThree {
	// 加锁超时时间，单位毫秒， 即：加锁时间内执行完操作，如果未完成会有并发现象
	private static final long LOCK_TIMEOUT = 5 * 1000;

	private static StringRedisTemplate redisTemplate;

	public DistributedLocksThree(StringRedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	/**
	 * 加锁 取到锁加锁，取不到锁一直等待知道获得锁
	 * 
	 * @param lockKey
	 * @param threadName
	 * @return
	 */

	public synchronized long lock(String lockKey, String threadName) {
		System.out.println(threadName + "开始执行加锁");
		while (true) { // 循环获取锁
			// 锁时间
			Long lock_timeout = currtTimeForRedis() + LOCK_TIMEOUT + 1;
			if (redisTemplate.execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
					// 定义序列化方式
					RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
					byte[] value = serializer.serialize(lock_timeout.toString());
					boolean flag = redisConnection.setNX(lockKey.getBytes(), value);
					return flag;
				}
			})) {
				// 如果加锁成功
				System.out.println(threadName + "加锁成功 ");
				// 设置超时时间，释放内存
				redisTemplate.expire(lockKey, LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
				return lock_timeout;
			} else {
				// 获取redis里面的时间
				String result = redisTemplate.opsForValue().get(lockKey);
				Long currt_lock_timeout_str = result == null ? null : Long.parseLong(result);
				// 锁已经失效
				if (currt_lock_timeout_str != null && currt_lock_timeout_str < System.currentTimeMillis()) {
					// 判断是否为空，不为空时，说明已经失效，如果被其他线程设置了值，则第二个条件判断无法执行
					// 获取上一个锁到期时间，并设置现在的锁到期时间
					Long old_lock_timeout_Str = Long
							.valueOf(redisTemplate.opsForValue().getAndSet(lockKey, lock_timeout.toString()));
					if (old_lock_timeout_Str != null && old_lock_timeout_Str.equals(currt_lock_timeout_str)) {
						// 多线程运行时，多个线程签好都到了这里，但只有一个线程的设置值和当前值相同，它才有权利获取锁
						System.out.println(threadName + "加锁成功 ");
						// 设置超时间，释放内存
						redisTemplate.expire(lockKey, LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

						// 返回加锁时间
						return lock_timeout;
					}
				}
			}

			try {
				System.out.println(threadName + "等待加锁， 睡眠100毫秒");
//	        TimeUnit.MILLISECONDS.sleep(100); 
				TimeUnit.MILLISECONDS.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 解锁
	 * 
	 * @param lockKey
	 * @param lockValue
	 * @param threadName
	 */
	public synchronized void unlock(String lockKey, long lockValue, String threadName) {
		System.out.println(threadName + "执行解锁==========");// 正常直接删除 如果异常关闭判断加锁会判断过期时间
		// 获取redis中设置的时间
		String result = redisTemplate.opsForValue().get(lockKey);
		Long currt_lock_timeout_str = result == null ? null : Long.valueOf(result);

		// 如果是加锁者，则删除锁， 如果不是，则等待自动过期，重新竞争加锁
		if (currt_lock_timeout_str != null && currt_lock_timeout_str == lockValue) {
			redisTemplate.delete(lockKey);
			System.out.println(threadName + "解锁成功------------------");
		}
	}

	/**
	 * 多服务器集群，使用下面的方法，代替System.currentTimeMillis()，获取redis时间，避免多服务的时间不一致问题！！！
	 * @return
	 */
	public long currtTimeForRedis() {
		return redisTemplate.execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
				return redisConnection.time();
			}
		});
	}

	
	private static final String LOCK_NO = "redis_distribution_lock_no_";
	private static int i = 0;
	private static void task(String name) {
//		    System.out.println(name + "任务执行中"+(i++)); 
		// 创建一个redis分布式锁
		DistributedLocksThree redisLock = new DistributedLocksThree(redisTemplate);
		// 加锁时间
		Long lockTime;
		if ((lockTime = redisLock.lock((LOCK_NO + 1) + "", name)) != null) {
			// 开始执行任务
			System.out.println(name + "任务执行中" + (i++));
			// 任务执行完毕 关闭锁
			redisLock.unlock((LOCK_NO + 1) + "", lockTime, name);
		}
	}
	
	public static void main(String[] args) {
		/**
		 * 模拟1000个线程同时执行业务，修改资源 使用线程池定义了20个线程
		 * 
		 */
		 ExecutorService service = Executors.newFixedThreadPool(20);

		for (int i = 0; i < 1000; i++) {
			service.execute(new Runnable() {
				@Override
				public void run() {
					task(Thread.currentThread().getName());
				}
			});
		}	
	}

}