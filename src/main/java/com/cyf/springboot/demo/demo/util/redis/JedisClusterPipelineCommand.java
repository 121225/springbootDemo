package com.cyf.springboot.demo.demo.util.redis;

import com.cyf.springboot.demo.demo.util.field.GetFieldValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.JedisClusterCRC16;

import java.lang.reflect.Field;
import java.util.*;

/**
 * created by CAIYANFENG on 23/02/2022
 */
public abstract class JedisClusterPipelineCommand {
    private static final Logger logger = LoggerFactory.getLogger(JedisClusterPipelineCommand.class);

    private static final Field FIELD_CONNECTION_HANDLER;
    private static final Field FIELD_CACHE;
    private static final Field FIELD_REDIRECTIONS;

    static {
        FIELD_CONNECTION_HANDLER = GetFieldValueUtil.getField(BinaryJedisCluster.class, "connectionHandler");
        FIELD_CACHE = GetFieldValueUtil.getField(JedisClusterConnectionHandler.class, "cache");
        FIELD_REDIRECTIONS = GetFieldValueUtil.getField(BinaryJedisCluster.class, "maxAttempts");
    }

    private JedisClusterConnectionHandler connectionHandler;
    private JedisClusterInfoCache clusterInfoCache;
    private int redirections;

    public JedisClusterPipelineCommand (JedisCluster jedisCluster){
        connectionHandler = GetFieldValueUtil.getValue(jedisCluster,FIELD_CONNECTION_HANDLER);
        clusterInfoCache = GetFieldValueUtil.getValue(connectionHandler,FIELD_CACHE);
        redirections = GetFieldValueUtil.getValue(jedisCluster,FIELD_REDIRECTIONS);
    }

    public abstract void excute(Pipeline pipeline,String key);

    public List<Object> run(List<String> keys){
        if (keys == null || keys.size() == 0){
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster");
        } else {
            return this.runWithRetries(this.redirections,keys);
        }
    }

    private List<Object> runWithRetries(int redirections,List<String> keys){
        if (redirections <= 0){
            throw new JedisClusterMaxRedirectionsException("Too many cluster redirecions");
        }
        List<Object> resList = new ArrayList<>();
        List<String> keyList;
        JedisPool currentJedisPool = null;
        Pipeline currentPipeline;
        List<Object> res = new ArrayList<>();
        Map<String,Object> resultMap = new HashMap<>();

        //获取JedisPool和key的映射关系
        Map<JedisPool,List<String>> jedisPoolMap = getJedisPool(keys);
        for (Map.Entry<JedisPool, List<String>> entry : jedisPoolMap.entrySet()) {
            Jedis jedis = null;
            try {
                currentJedisPool = entry.getKey();
                keyList = entry.getValue();
                //获取pipeline
                jedis = currentJedisPool.getResource();
                currentPipeline = jedis.pipelined();
                for (String key : keyList) {
                    excute(currentPipeline,key);
                }
                //从pipeline中获取结果
                res = currentPipeline.syncAndReturnAll();
                currentPipeline.close();
                for (int i = 0; i < keyList.size(); i++) {
                    if (null == res.get(i)) {
                        resultMap.put(keyList.get(i), null);
                    } else {
                        resultMap.put(keyList.get(i), res.get(i));
                    }
                }
            } catch (Exception e) {
                logger.error("JedisClusterPipelineCommand runWithRetries err",e);
                if (e instanceof JedisMovedDataException) {
                    this.connectionHandler.renewSlotCache();
                    this.runWithRetries(redirections-1,keys);
                }
            } finally {
                returnResource(jedis, currentJedisPool);
            }
        }
        resList = sortList(keys, resultMap);
        return resList;
    }


    /**
     * 根据Key获取jedisPool
     * @param keys
     * @return
     */
    private Map<JedisPool, List<String>> getJedisPool(List<String> keys) {
        //JedisCluster继承了BinaryJedisCluster
        //BinaryJedisCluster的JedisClusterConnectionHandler属性
        //里面有JedisClusterInfoCache，根据这一条继承链，可以获取到JedisClusterInfoCache
        //从而获取slot和JedisPool直接的映射
        //保存地址+端口和命令的映射
        Map<JedisPool, List<String>> jedisPoolMap = new HashMap<>();

        JedisPool currentJedisPool = null;
        List<String> keyList;
        for (String key : keys) {
            //计算哈希槽
            int crc = JedisClusterCRC16.getSlot(key);
            //通过哈希槽获取节点的连接
            currentJedisPool = clusterInfoCache.getSlotPool(crc);

            //由于JedisPool作为value保存在JedisClusterInfoCache中的一个map对象中，每个节点的
            //JedisPool在map的初始化阶段就是确定的和唯一的，所以获取到的每个节点的JedisPool都是一样
            //的，可以作为map的key
            if (jedisPoolMap.containsKey(currentJedisPool)) {
                jedisPoolMap.get(currentJedisPool).add(key);
            } else {
                keyList = new ArrayList<>();
                keyList.add(key);
                jedisPoolMap.put(currentJedisPool, keyList);
            }
        }
        return jedisPoolMap;
    }

    /**
     * 组装返回结果
     * @param keys
     * @param params
     * @return
     */
    private List<Object> sortList(List<String> keys, Map<String, Object> params) {
        List<Object> resultList = new ArrayList<>();
        Iterator<String> it = keys.iterator();
        while (it.hasNext()) {
            String key = it.next();
            resultList.add(params.get(key));
        }
        return resultList;
    }

    /**
     * 释放jedis资源
     *
     * @param jedis
     */
    public void returnResource(Jedis jedis, JedisPool jedisPool) {
        if (jedis != null && jedisPool != null) {
            jedisPool.returnResource(jedis);
        }
    }
}
