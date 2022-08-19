package com.fanfan.flink.day04_1;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月16日 13时54分
 * @Version: v1.0
 * TODO 连续1s 温度上升检测
 */
public class Example1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                .keyBy(r -> r.SensorID)
                .process(new MyAlterInfo())
                .print();



        env.execute();
    }
    // 温度连续1s上升,报警通知
    public static class MyAlterInfo extends KeyedProcessFunction<String,SensorReading,String>{
        // 值状态变量 保存上一次记录温度值、定时器标记
        private ValueState<Double> prevTemp;
        private ValueState<Integer> ts;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化值状态变量
            prevTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                    "prevTemp",
                    Types.DOUBLE
            ));

            ts = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                    "timer-flag",
                    Types.INT
            ));
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            // 如果值状态变量的value为空，则注册定时器、将第一次收集的温度作为prevTemp
            if(prevTemp.value() == null){
                prevTemp.update(in.SensorTemperature);
            }

        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {

        }
    }

    // 传感器数据-数据源
    public static class SensorSource extends RichSourceFunction<SensorReading> {
        private boolean running = true;
        // 温度的模拟
        private Random random;
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while(running){
                // 传感器ID sensor_1 -- sensor_4
                for (int i = 1; i < 5; i++) {
                    ctx.collect(new SensorReading(
                            "sensor_" + i,
                            random.nextGaussian() //温度
                    ));
                }
                // 300ms 传感器发送一次温度
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // 模拟传感器的POJO类
    public static class SensorReading{
        // 传感器ID
        public String SensorID;

        // 传感器温度
        public Double SensorTemperature;

        public SensorReading() {
        }

        public SensorReading(String sensorID, Double sensorTemperature) {
            SensorID = sensorID;
            SensorTemperature = sensorTemperature;
        }
    }
}
