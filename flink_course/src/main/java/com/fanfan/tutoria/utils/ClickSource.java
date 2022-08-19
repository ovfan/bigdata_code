package com.fanfan.tutoria.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @ClassName: ClickSource
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 14时09分
 * @Version: v1.0
 */
//单并行度的数据源，并行度只能设置为1
public class ClickSource implements SourceFunction<ClickEvent> {
    private boolean running = true;

    // 随机数发生器
    private Random random = new Random();

    private String[] userArr = {"mary","alice","fan"};
    private String[] urlArr = {"./home","./cart","./buy"};
    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        while (running){
            ctx.collect(new ClickEvent(
                    // nextInt(3) --随机生成 0,1,2  注意：3 不包含
                    userArr[random.nextInt(userArr.length)],
                    urlArr[random.nextInt(urlArr.length)],
                    // 当前系统时间的毫秒时间戳
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 控制发送数据的 速度
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
