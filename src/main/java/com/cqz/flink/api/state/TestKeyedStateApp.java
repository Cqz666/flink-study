package com.cqz.flink.api.state;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

public class TestKeyedStateApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> input = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> dataStream = input.map(value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        resultStream.print();

        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading,Tuple3<String,Double,Double>>{
        //温度跳变阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        //定义状态，保持上次温度
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp",Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            //获取上次温度值
            Double lastTemp = lastTempState.value();
            if (lastTemp != null){
                Double diff = Math.abs(value.getTemperature()-lastTemp);
                if (diff>=threshold){
                    out.collect(new Tuple3<>(value.getId(),lastTemp,value.getTemperature()));
                }
            }
            //更新状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }


}
