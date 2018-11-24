package com.ranx.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import com.ranx.utils.RedisUtil;

/**
 * @Description 
 * @author ranx
 * @date 2018年11月22日 下午3:57:08
 *
 */
@Component("redisObjectService")
public class RedisObjectService {

	@Autowired
	RedisUtil redisUtil;
	
	public boolean setValue(String key, String value) {
		try {
			redisUtil.set(key, value);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	public String getValue(String key) {
		try {
			String value = (String)redisUtil.get(key);
			return value;
		} catch (Exception e) {
			return "读取缓存出错！";
		}
	}
}
