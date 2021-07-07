package com.cqz.flink.api.udf;

import com.alibaba.fastjson.JSON;
import com.cqz.flink.api.model.EventUser;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class EventUserRedisMapper implements RedisMapper<EventUser> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    @Override
    public String getKeyFromData(EventUser eventUser) {
        return eventUser.getVid();
    }

    @Override
    public String getValueFromData(EventUser eventUser) {
        return JSON.toJSONString(eventUser) ;
    }
}
