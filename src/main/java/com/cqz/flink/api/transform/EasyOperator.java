package com.cqz.flink.api.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Objects;
@Slf4j
public class EasyOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = env.readTextFile(EasyOperator.class.getClassLoader().getResource("source.txt").toString());
//        DataStream<String> source = env.readTextFile("/usr/local/flink-1.12.2/log/source.txt");
        //1.map算子
        DataStream<Integer> mapStream = source.map((MapFunction<String, Integer>) String::length);
        //2.flatmap算子
        DataStream<String> flatMapStream = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] fields = value.split(",");
                    for (String field : fields) {
                        out.collect(field);
                    }
            }
        });
        //filter算子
        DataStream<String> filterStream = source.filter((FilterFunction<String>) s -> s.startsWith("sensor_1"));
        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
