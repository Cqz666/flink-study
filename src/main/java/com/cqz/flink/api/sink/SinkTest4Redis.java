package com.cqz.flink.api.sink;

import com.cqz.flink.api.beans.SensorReading;
import com.cqz.flink.api.model.EventUser;
import com.cqz.flink.api.udf.EventUserRedisMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.text.DateFormat;
import java.util.Date;
import java.util.Objects;

public class SinkTest4Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.readTextFile(Objects.requireNonNull(SinkTest4Redis.class.getClassLoader().getResource("eventuser.txt")).toString());

        DataStream<EventUser> dataStream = input.map(value -> {
            String[] filed = value.split(",");
            return new EventUser(filed[0], filed[1], new Long(filed[2]),filed[3],DateFormat.getDateInstance().parse(filed[4]));
        });
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
         dataStream.addSink(new RedisSink<>(config, new EventUserRedisMapper()));

        //kafka
//        DataStream<String> dataStream2 = input.map( value -> {
//            String[] filed = value.split(",");
//            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2])).toString();
//        });
//        dataStream2.addSink(new FlinkKafkaProducer011<>("localhost:9092","sinktest",new SimpleStringSchema()));
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


