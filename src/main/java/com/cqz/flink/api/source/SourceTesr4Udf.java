package com.cqz.flink.api.source;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTesr4Udf {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> source = env.addSource(new MySensorSource());
        source.print();
        env.execute();
    }

    private static class MySensorSource implements SourceFunction<SensorReading>{

        private boolean isRunning = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            Random random = new Random();
            HashMap<String, Double> sensorTemp = new HashMap<>();
            //设置10个传感器的初始温度
            for (int i = 0; i < 10; i++) {
                sensorTemp.put("sensor_"+(i+1),60+random.nextGaussian()*20);
            }

            while (isRunning){
                for (String sensorId : sensorTemp.keySet()) {
                    //在初始温度基础上随机波动
                    double newTemp = sensorTemp.get(sensorId) + random.nextGaussian();
                    sensorTemp.put(sensorId,newTemp);
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning=false;
        }
    }

}
