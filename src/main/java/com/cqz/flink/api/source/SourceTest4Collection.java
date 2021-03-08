package com.cqz.flink.api.source;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest4Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1224113455L, 31.6),
                new SensorReading("sensor_2", 1224141233L, 31.5),
                new SensorReading("sensor_3", 1536533356L, 31.7)
        ));
        source.print("data");

        env.execute();

    }

}
