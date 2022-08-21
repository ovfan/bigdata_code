package com.fanfan.tutoria.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName: Example4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月21日 14时31分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new TemperatureSource())
                .keyBy(r -> r.SensorID)
                .process(new AlterTempperature())
                .print();
        env.execute();
    }

    //
    public static class AlterTempperature extends KeyedProcessFunction<String, SensorReading, String> {
        // TODO 实现连续1s 温度上升检测
        private ValueState<Long> timeTs; // 保存定时器的时间戳
        private ValueState<Double> prevTemp; // 保存上一次的温度值

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化值状态变量
            timeTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "time-ts",
                    Types.LONG
            ));
            prevTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                    "prev-temp",
                    Types.DOUBLE
            ));
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            // 保存上一次的传感器的温度值
            Double prevSensorTemp = prevTemp.value();
            // 报警定时器的时间戳
            Long ts = timeTs.value();

            // 当前温度 保存到prevTemp中 这个要提出来---每来一次传感器的数据，就更新温度值
            prevTemp.update(in.temperature);

            // 如果当前温度 > prevSensorTemp 且 定时器不存在
            if (prevSensorTemp != null) {
                if (in.temperature > prevSensorTemp && ts == null) {
                    long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                    // 注册 1s 之后的报警定时器
                    ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                    timeTs.update(oneSecondLater);
                } else if (in.temperature < prevSensorTemp && ts != null) {
                    // 删除报警定时器
                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    timeTs.clear();
                }
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey() + " 连续1s温度上升了");
            timeTs.clear();
        }
    }

    public static class SensorReading {
        public String SensorID;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorID, Double temperature) {
            SensorID = sensorID;
            this.temperature = temperature;
        }
    }

    public static class TemperatureSource implements SourceFunction<SensorReading> {
        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                for (int i = 1; i < 4; i++) {
                    ctx.collect(new SensorReading(
                            "Sensor" + i,
                            random.nextGaussian()
                    ));
                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
