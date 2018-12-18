package com.ranx.jedis;

import java.util.Set;

import redis.clients.jedis.Jedis;

/**
 * @author ranx
 * @create 2018-12-17 23:00
 **/
public class JedisApiTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    public static void main(String args[]) {
        Jedis jedis = new Jedis(HOST, PORT);
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        jedis.set("k3", "v3");
        //获取数据
        String value = jedis.get("name");
        System.out.println(value);

        Set<String> sets = jedis.keys("*");

        //释放资源
        jedis.close();

    }

    //主从复制
    public static void MasterSlaver() {
        Jedis jedis_M = new Jedis("127.0.0.1", 6379);
        Jedis jedis_S = new Jedis("127.0.0.1", 6380);

        jedis_S.slaveof("127.0.0.1", 6379);

        jedis_M.set("class","1122");
        String result = jedis_S.get("class");
        System.out.println(result);
    }
}
