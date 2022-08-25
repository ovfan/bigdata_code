package com.fanfan.tutoria.revise;

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
 * @ClassName: TemperatureAlter
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月25日 15时22分
 * @Version: v1.0
 */
public class TemperatureAlter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorID)
                .process(new TemperatureAlert())
                .print();

        env.execute();
    }

    public static class TemperatureAlert extends KeyedProcessFunction<String, SensorReading, String> {
        // 保存上一次的温度值
        private ValueState<Double> lastTemp;
        // 保存报警定时器的时间戳
        private ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>(
                            "last-temp",
                            Types.DOUBLE
                    )
            );

            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "timer-ts",
                    Types.LONG
            ));
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            // 获取上一次的温度值
            Double prevTemp = lastTemp.value();
            // 获取报警定时器的时间戳
            Long timer = timerTs.value();

            // 更新温度值
            lastTemp.update(in.temperature);

            if (prevTemp != null) {
                if (prevTemp > in.temperature && timer == null) {
                    // 注册报警定时器
                    long currTs = ctx.timerService().currentProcessingTime();
                    long oneSecondLater = currTs + 1000L;
                    ctx.timerService().registerProcessingTimeTimer(oneSecondLater);

                    timerTs.update(oneSecondLater);
                }else if (prevTemp < in.temperature && timer != null){
                    // 删除报警定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                    timerTs.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续1s温度上升了");
            timerTs.clear();
        }
    }


    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                for (int i = 1; i < 4; i++) {
                    ctx.collect(new SensorReading(
                            "sensor" + i,
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

    public static class SensorReading {
        public String sensorID;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorID, Double temperature) {
            this.sensorID = sensorID;
            this.temperature = temperature;
        }
    }
}
