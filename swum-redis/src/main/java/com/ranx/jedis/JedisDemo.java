package com.ranx.jedis;


import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author ranx
 * @create 2018-11-11 22:30
 **/
public class JedisDemo {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    /**
     * 单链接实例
     */
    @Test
    public void demo1() {
        //1.设置IP地址和端口
        Jedis jedis = new Jedis(HOST, PORT);
        //2.保存数据
        jedis.set("name", "ranx");
        //3.获取数据
        String value = jedis.get("name");
        System.out.println(value);
        //4.释放资源
        jedis.close();
    }

    /**
     * 连接池方式连接
     */
    @Test
    public void demo2() {
        //获得连接池的配置对象
        JedisPoolConfig config = new JedisPoolConfig();
        //设置最大连接数
        config.setMaxTotal(30);
        //设置最大空闲连接数
        config.setMaxIdle(10);
        //获得连接池
        JedisPool jedisPool = new JedisPool(config, HOST, PORT);
        //获得核心对象
        Jedis jedis = null;

        try {
            //通过连接池获得连接
            jedis = jedisPool.getResource();
            //设置数据
            jedis.set("name", "ranx");
            //3.获取数据
            String value = jedis.get("name");
            System.out.println(value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放资源
            if (jedis != null) {
                jedis.close();
            }
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }
}
