package com.cqz.flink.api.transform;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.readTextFile(EasyOperator.class.getClassLoader().getResource("source.txt").toString());

        DataStream<SensorReading> dataStream = input.map((MapFunction<String, SensorReading>) value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        DataStream<SensorReading> reduce = keyedStream.reduce(
                (ReduceFunction<SensorReading>) (sr1, sr2) ->
                        new SensorReading(sr1.getId(), sr2.getTimestamp(), Math.max(sr1.getTemperature(), sr2.getTemperature())));
        reduce.print();
        env.execute();
    }
}
