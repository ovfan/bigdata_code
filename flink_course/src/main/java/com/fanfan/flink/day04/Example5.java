package com.fanfan.flink.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @ClassName: Example5
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 14时07分
 * @Version: v1.0
 * TODO 检测连续1S温度上升报警预警功能
 * 需要：Sensor传感器（id,temperature）
 */
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensor_id)
                .process(new SensorTemperatureDetect())
                .print();


        env.execute();
    }

    // 传感器监测
    public static class SensorTemperatureDetect extends KeyedProcessFunction<String, Sensor, String> {
        // TODO 监测连续一秒的数据变化，所以需要用到定时器
        // 思路：
        // 温度出现上升，注册1秒钟之后的报警定时器
        // 温度出现下降，如果报警定时器还存在，那么说明是1秒钟之内出现了温度下降
        // 删除报警定时器

        // 保存上一次的温度值
        private ValueState<Double> lastTemp;
        // 1. 保存报警定时器的时间戳
        // 2. 如果不为空，表示当前存在报警定时器
        private ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                    "prevTemp",
                    Types.DOUBLE
            ));

            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "timerTs",
                    Types.LONG
            ));
        }

        @Override
        public void processElement(Sensor in, Context ctx, Collector<String> out) throws Exception {
            // 首先取出上一次的温度值
            // 如果到达的数据是第一条温度，那么prevTemp是null
            Double prevTemp = lastTemp.value();

            //将当前温度值保存到prevTemp;
            lastTemp.update(in.temperature);
            // 取出报警定时器的时间戳
            Long ts = timerTs.value();

            // 注册与取消报警定时器的逻辑
            if (prevTemp != null) { //上一次的温度不为null
                // 1. 如果温度上升 且 报警定时器不存在
                if (in.temperature > prevTemp && ts == null) {
                    //获取当前处理时间
                    long currTs = ctx.timerService().currentProcessingTime();
                    //注册1s之后的定时器
                    ctx.timerService().registerProcessingTimeTimer(currTs + 1000L);
                    timerTs.update(currTs + 1000L);
                } else if (in.temperature < prevTemp && ts != null) {
                    // 手动从定时器队列中删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                    // 将保存的定时器的时间戳从状态变量timerTs中删除
                    timerTs.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //向下游报警的信息
            out.collect(ctx.getCurrentKey() + "已经连续一秒温度上升了！" + "当前时间为" + new Timestamp(timerTs.value()));

            timerTs.clear();
        }
    }

    //TODO 实现传感器的POJO类与传感器的自定义数据源
    public static class Sensor {
        public String sensor_id; //传感器ID
        public Double temperature; //传感器温度

        public Sensor() {
        }

        public Sensor(String sensor_id, Double temperature) {
            this.sensor_id = sensor_id;
            this.temperature = temperature;
        }
    }

    public static class SensorSource implements SourceFunction<Sensor> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Sensor> out) throws Exception {
            while (running) {
                for (int i = 1; i < 5; i++) {
                    out.collect(new Sensor(
                            "sensor_id_" + i,
                            random.nextGaussian()   // 随机的数据更加密集
                    ));
                }
                Thread.sleep(200L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
