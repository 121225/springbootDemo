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

        //??????JedisPool???key???????????????
        Map<JedisPool,List<String>> jedisPoolMap = getJedisPool(keys);
        for (Map.Entry<JedisPool, List<String>> entry : jedisPoolMap.entrySet()) {
            Jedis jedis = null;
            try {
                currentJedisPool = entry.getKey();
                keyList = entry.getValue();
                //??????pipeline
                jedis = currentJedisPool.getResource();
                currentPipeline = jedis.pipelined();
                for (String key : keyList) {
                    excute(currentPipeline,key);
                }
                //???pipeline???????????????
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
     * ??????Key??????jedisPool
     * @param keys
     * @return
     */
    private Map<JedisPool, List<String>> getJedisPool(List<String> keys) {
        //JedisCluster?????????BinaryJedisCluster
        //BinaryJedisCluster???JedisClusterConnectionHandler??????
        //?????????JedisClusterInfoCache?????????????????????????????????????????????JedisClusterInfoCache
        //????????????slot???JedisPool???????????????
        //????????????+????????????????????????
        Map<JedisPool, List<String>> jedisPoolMap = new HashMap<>();

        JedisPool currentJedisPool = null;
        List<String> keyList;
        for (String key : keys) {
            //???????????????
            int crc = JedisClusterCRC16.getSlot(key);
            //????????????????????????????????????
            currentJedisPool = clusterInfoCache.getSlotPool(crc);

            //??????JedisPool??????value?????????JedisClusterInfoCache????????????map???????????????????????????
            //JedisPool???map?????????????????????????????????????????????????????????????????????????????????JedisPool????????????
            //??????????????????map???key
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
     * ??????????????????
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
     * ??????jedis??????
     *
     * @param jedis
     */
    public void returnResource(Jedis jedis, JedisPool jedisPool) {
        if (jedis != null && jedisPool != null) {
            jedisPool.returnResource(jedis);
        }
    }
}
