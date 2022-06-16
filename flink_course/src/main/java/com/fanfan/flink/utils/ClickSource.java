package com.fanfan.flink.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @ClassName: ClickSource
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月15日 15时32分
 * @Version: v1.0
 */

// TODO --自定义数据源,需要实现SourceFunction<E>
public class ClickSource implements SourceFunction<ClickEvent> {
    private boolean running = true;
    // 模拟数据
    Random random = new Random();
    private String[] userArr = {"fanfan","qiqi","peipei"};
    private String[] userUrl = {"./home","./cart","./buy"};

    // run方法不断的发送数据
    @Override
    public void run(SourceContext<ClickEvent> cxt) throws Exception {
        while (running){
            cxt.collect(new ClickEvent(userArr[random.nextInt(userArr.length)],
                    userUrl[random.nextInt(userUrl.length)],
                    // 获取当前的机器时间，作为事件的事件时间
                    Calendar.getInstance().getTimeInMillis()
                    ));
            // 一秒发送一次
            Thread.sleep(1000L);
        }
    }

    // 取消发送数据
    @Override
    public void cancel() {
        running = false;
    }
}
