package com.cyf.springboot.demo.demo.util.redis;

import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;

/**
 * created by CAIYANFENG on 16/01/2022
 * jedis管道操作
 * 批量操作
 */
public class JedisBatch {

    @Autowired
    private JedisCluster jedisCluster;

    /**
     * 使用pipeline批量操作
     * @param keys
     * @return
     */
    Map<String,String> batchGetByPipeline(List<String> keys){
        jedisCluster.mget("a");
        return null;
    }
}
