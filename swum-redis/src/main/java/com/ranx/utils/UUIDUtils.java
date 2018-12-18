package com.ranx.utils;

import java.util.UUID;

/**
 * @Description UUID工具类
 * @author ranx
 * @date 2018年12月18日 上午11:49:53
 *
 */
public class UUIDUtils {
	public static String getUUID() {
		//格式化UUID,去掉"-"
		return UUID.randomUUID().toString().replace("-", "");
	}
	
	public static void main(String[] args) {
        System.out.println("格式前的UUID ： " + UUID.randomUUID().toString());
        System.out.println("格式化后的UUID ：" + getUUID());
    }

}
