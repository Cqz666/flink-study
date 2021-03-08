package com.cqz.flink.api.sink;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class SinkTest4Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.readTextFile(SinkTest4Kafka.class.getClassLoader().getResource("source.txt").toString());

        DataStream<String> dataStream = input.map( value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<>("localhost:9092","sinktest",new SimpleStringSchema()));

        env.execute();
    }
}
