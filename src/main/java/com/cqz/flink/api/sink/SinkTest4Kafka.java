package com.cqz.flink.api.sink;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class SinkTest4Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
        DataStream<String> input = env.readTextFile(SinkTest4Kafka.class.getClassLoader().getResource("source.txt").toString());
        input.print("source");
        DataStream<String> dataStream = input.map( value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer<>("localhost:9092", "sinktest", new SimpleStringSchema()));
        dataStream.print("test");
        env.execute();
    }
}
