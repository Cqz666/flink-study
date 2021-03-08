package com.cqz.flink.api.sink;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Objects;

public class SinkTest4Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.readTextFile(Objects.requireNonNull(SinkTest4Redis.class.getClassLoader().getResource("source.txt")).toString());

        DataStream<SensorReading> dataStream = input.map( value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        });
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
        dataStream.addSink(new RedisSink<>(config,new MyRedisMapper()));
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<SensorReading>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return String.valueOf(sensorReading.getTemperature());
        }
    }

}


