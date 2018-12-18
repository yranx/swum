package com.ranx.distributedLocks;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.StringUtils;

import com.ranx.utils.RedisUtil;

/**
 * @Description redis实现分布式锁
 * 思路清新，实现略有复杂，需要等后面验证
 * @author ranx
 * @date 2018年12月18日 上午9:32:35
 *https://blog.csdn.net/wang258533488/article/details/78913800
 */
public class DistributedLocks {
	private static RedisTemplate redisTemplate;
	private static RedisUtil redisUtil;

	// 如果为空就插入值，返回true,不为空则不做操作，返回false
	public static boolean setIfAbsent(String key, String value) {
		return redisTemplate.opsForValue().setIfAbsent(key, value);
	}

	// 插入新值，并返回旧值
	public static String getAndSet(String key, String value) {
		return (String) redisTemplate.opsForValue().getAndSet(key, value);
	}


	// 获取分布式锁，获取不成功会一直尝试获取
 	public static boolean getDistributedLock(String redisKey) {
		try {
			String currentMill = System.currentTimeMillis() + "";
			while (true) {
				Boolean getLock = DistributedLocks.setIfAbsent(redisKey, currentMill);
				if (getLock) {
					System.out
							.println(Thread.currentThread().getName() + "-成功获取分布式锁【" + redisKey + "】");
					return true;
				} else {
					System.out.println(
							Thread.currentThread().getName() + "-获取分布式锁【" + redisKey + "】失败，将尝试重新获取锁");
					while (true) {
						String currentLockValue = redisUtil.getStr(redisKey);
						long currentLockTime, oldLockTime;
						if (!StringUtils.isEmpty(currentLockValue)) {
							currentLockTime = Long.parseLong(currentLockValue);
							//timeout = ConfigUtil.getDistributeLockTimeOutValue() = 1000
							if (System.currentTimeMillis() - currentLockTime 
									> 1000) {
								// 锁过期
								System.out.println(Thread.currentThread().getName() + "-当前的分布式锁【"
										+ redisKey + "】已经超时，将尝试强制获取锁");
								String oldLockValue = DistributedLocks.getAndSet(redisKey,
										System.currentTimeMillis() + "");
								if(!StringUtils.isEmpty(oldLockValue)) {
									oldLockTime = Long.parseLong(oldLockValue);
									if (currentLockTime == oldLockTime) {
										//获得锁
										System.out.println(Thread.currentThread().getName() + "-成功获取分布式锁【"+ redisKey +"】");
										return true;
									} else {
										//在这之前已经有其他客户端获取锁，等待重试
										System.out.println(Thread.currentThread().getName() + 
												"-已经有其他客户端抢先获取到分布式锁【"+ redisKey +"】，将稍后重试获取锁");
										try {
	                                        Thread.sleep(50);
	                                    } catch (InterruptedException e) {
	                                        e.printStackTrace();
	                                    }
									}
								} else {
									//锁被释放，可以获得锁
	                                System.out.println(Thread.currentThread().getName() + "-当前分布式锁【"+ redisKey +"】已经被释放，将尝试重新获取锁");
	                                break;
								}
							} else {
								//锁正在有效期被其他客户端使用，等待重试
								System.out.println(Thread.currentThread().getName() + "-分布式锁【"+ redisKey +"】正在被其他客户端使用，将稍后重试获取锁");
								try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
							}
						} else {
							//锁被释放，可以获得锁
                            System.out.println(Thread.currentThread().getName() + "-当前分布式锁【"+ redisKey +"】已经被释放，将尝试重新获取锁");
                            break;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return true;
		}
	}

	/**
	 * 尝试获取分布式锁，获取失败则直接返回
	 * @param redisKey
	 * @return
	 */
	public static boolean tryGetDistributedLock(String redisKey){
	    String currentMill=  System.currentTimeMillis() + "";
	    Boolean getLock =DistributedLocks.setIfAbsent(redisKey, currentMill);
	    if(getLock){
	    	System.out.println(Thread.currentThread().getName() + "-成功获取分布式锁【"+ redisKey +"】");
	        return true;
	    }
	    System.out.println(Thread.currentThread().getName() + "-尝试获取分布式锁【"+ redisKey +"】失败");
	    return false;
	}  

	/**
	 * 释放分布式锁
	 * 可能会出现释放不成功的情况，不需处理，只需等待自动超时即可
	 * @param redisKey
	 * @return
	 */
	public static boolean releaseDistributedLock(String redisKey){
		redisUtil.remove(redisKey);
	    System.out.println(Thread.currentThread().getName() + "-成功释放分布式锁【"+ redisKey +"】");
	    return true;
	}
}
