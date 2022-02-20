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
        /**
         * cluster中如果计算出来不在同一个槽位不允许使用mget，mset等批量操作命令
         * 需要批量操作需要调用jediscluster中代码计算每一个key对应的槽位，根据槽位计算node，再通过
         * 再对涉及到的每个node使用pipeline发送批量处理命令
         * github中找代码
         * mo-service中已实现
         * jedisCluster.mget("a");*/
        return null;
    }
}
