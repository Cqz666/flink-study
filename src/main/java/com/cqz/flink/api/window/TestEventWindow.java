package com.cqz.flink.api.window;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class TestEventWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> input = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> dataStream = input.map(value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        })
                //乱序数据设置时间戳和watermarks
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp()*1000L;
            }
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};

        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream
                .keyBy("id")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
