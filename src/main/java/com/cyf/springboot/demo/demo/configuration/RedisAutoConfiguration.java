package com.cyf.springboot.demo.demo.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * created by CAIYANFENG on 16/01/2022
 */
@Configuration
@ConditionalOnClass({JedisCluster.class})
public class RedisAutoConfiguration {

    @Value("${spring.redis.cluster.nodes}")
    private String clusterNodes;
    @Value("${spring.redisCluster.timeout}")
    private int timeout;
    @Value("${spring.redis.pool.max-idle}")
    private int maxIdle;
    @Value("${spring.redis.pool.max-wait}")
    private long maxWaitMillis;
    @Value("${spring.redis.commandTimeout}")
    private int commandTimeout;
    @Value("${spring.redis.password}")
    private String password;

    @Bean
    public JedisCluster getJedisCluster() {
        String[] cNodes = clusterNodes.split(",");
        Set<HostAndPort> nodes = new HashSet<>();
        //分割出集群节点
        for (String node : cNodes) {
            String[] hp = node.split(":");
            nodes.add(new HostAndPort(hp[0], Integer.parseInt(hp[1])));
        }
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMaxWaitMillis(maxWaitMillis);
        //建立集群对象。没有密码的请使用这一个
        return new JedisCluster(nodes,commandTimeout);
        //有密码的请使用这一个。 我这里是redis有密码的因此我使用的这一个
        //return new JedisCluster(nodes,commandTimeout,commandTimeout,5,password, jedisPoolConfig);
    }
}
