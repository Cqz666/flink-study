package com.cqz.flink.api.window;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

public class TestCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> input = env.readTextFile(Objects.requireNonNull(SinkTest4Redis.class.getClassLoader().getResource("source.txt")).toString());
        env.setParallelism(1);
        DataStream<String> input = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> dataStream = input.map(value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        });

        SingleOutputStreamOperator<Double> avgTempStream = dataStream.keyBy("id").countWindow(10, 2)
                .aggregate(new MyAvgTemp());

        avgTempStream.print();

        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double,Integer>,Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator._1+value.getTemperature(),accumulator._2+1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator._1/accumulator._2;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a._1+b._1,a._2+b._2);
        }
    }
}
