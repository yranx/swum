package com.ranx.utils;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

/**
 * @Description 
 * @author ranx
 * @date 2018年11月22日 下午3:21:20
 *
 */

public class RedisUtil {
	/**
	 * RedisTemplate:简化redis数据访问的帮助类，对Redis命令进行高级封装，
	 * 通过此类可调用ValueOperations和ListOperations等等方法
	 */
	@Autowired
	private RedisTemplate<Serializable, Object> redisTemplate;

	
	//批量删除key
	public void removePattern(final String pattern) {
		Set<Serializable> keys = redisTemplate.keys(pattern);
		if (keys.size() > 0) {
			redisTemplate.delete(keys);
		}
	}
	
	//删除对应的value
	public void remove(final String key) {
		if (exists(key)) {
			redisTemplate.delete(key);
		}
	}
	
	private boolean exists(final String key) {
		return redisTemplate.hasKey(key);
	}

	//批量删除对应value
	public void remove(final String... keys) {
		for (String key : keys) {
			remove(key);
		}
	}
	
	//读取缓存
	public Object get(final String key) {
//		ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
//		return operations.get(key);
		return redisTemplate.opsForValue().get(key);
	}
	
	public Object get(final String key, final String hashKey) {
		return redisTemplate.opsForHash().get(key, hashKey);
	}
	
	//写入缓存
	public boolean set(final String key, Object value) {
		boolean result = false;
		try {
			redisTemplate.opsForValue().set(key, value);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public boolean set(final String key, final String hashKey, Object value) {
		boolean result = false;
		try {
			redisTemplate.opsForHash().put(key, hashKey, value);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public boolean set(final String key, Object value, Long expireTime) {
		boolean result = false;
		try {
			redisTemplate.opsForValue().set(key, value);
			redisTemplate.expire(key, expireTime, TimeUnit.SECONDS);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
