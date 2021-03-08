package com.cqz.flink.api.sink;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class SinkTest4Kafka2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        DataStream<String> input = env.addSource(new FlinkKafkaConsumer011<>("sensor", new SimpleStringSchema(), properties));

        DataStream<String> dataStream = input.map( value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<>("localhost:9092","sinktest",new SimpleStringSchema()));

        env.execute();
    }
}
