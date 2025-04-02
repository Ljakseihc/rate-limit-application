package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.util.Set;

@Component
public class JedisRateLimitService implements RateLimitService {

    private final JedisCluster jedisCluster;
    private final Set<RateLimitRule> rateLimitRules;
    private final StatefulRedisClusterConnection<String,String> lettuceClusterConnection;

    @Autowired
    public JedisRateLimitService(JedisCluster jedisCluster, Set<RateLimitRule> rateLimitRules, StatefulRedisClusterConnection<String, String> lettuceClusterConnection) {
        this.jedisCluster = jedisCluster;
        this.rateLimitRules = rateLimitRules;
        this.lettuceClusterConnection = lettuceClusterConnection;
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {
//        var commands = lettuceClusterConnection.sync();
        for (RequestDescriptor descriptor : requestDescriptors) {
            for (RateLimitRule rule : rateLimitRules) {
                if (ruleMatches(descriptor, rule)) {
                    String redisKey = buildRedisKey(descriptor, rule);

                    int ttl = getTimeIntervalInSeconds(rule.getTimeInterval());
                    String count = jedisCluster.get(redisKey);

                    if (count == null) {
                        jedisCluster.setex(redisKey, ttl, "1");
                    } else {
                        long currentCount = Long.parseLong(count);
                        if (currentCount >= rule.getAllowedNumberOfRequests()) {
                            return true;
                        }
//                        commands.incr(redisKey);
                        jedisCluster.incr(redisKey);
                    }
                }
            }
        }
        return false;
    }

    private boolean ruleMatches(RequestDescriptor descriptor, RateLimitRule rule) {
        boolean matchesAccountId = matchesValue(rule.getAccountId().orElse(""), descriptor.getAccountId().orElse(""));
        boolean matchesClientIp = matchesValue(rule.getClientIp().orElse(""), descriptor.getClientIp().orElse(""));
        boolean matchesRequestType = matchesValue(rule.getRequestType().orElse(""), descriptor.getRequestType().orElse(""));

        return matchesAccountId && matchesClientIp && matchesRequestType;
    }

    private boolean matchesValue(String ruleVal, String clientVal) {
        if(ruleVal.isEmpty()){
            return true;
        }
        return ruleVal.equals(clientVal);
    }

    private String buildRedisKey(RequestDescriptor descriptor, RateLimitRule rule) {
        var key = new StringBuilder("ratelimit:");
        key.append(rule.getTimeInterval().name().toLowerCase()).append(":");

        descriptor.getAccountId().ifPresent(accountId -> key.append("accountId:").append(accountId).append(":"));
        descriptor.getClientIp().ifPresent(clientIp -> key.append("clientIp:").append(clientIp).append(":"));
        descriptor.getRequestType().ifPresent(requestType -> key.append("requestType:").append(requestType));

        if (key.charAt(key.length() - 1) == ':') {
            key.setLength(key.length() - 1);
        }

        return key.toString();
    }

    private int getTimeIntervalInSeconds(RateLimitTimeInterval interval) {
        return switch (interval) {
            case MINUTE -> 60;
            case HOUR -> 3600;
        };
    }


}
