package com.fanfan.tutoria.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName: IntSource
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 17时06分
 * @Version: v1.0
 */
public class IntSource implements SourceFunction<IntPOJO> {
    private boolean running = true;
    private Random random = new Random();
    @Override
    public void run(SourceContext<IntPOJO> ctx) throws Exception {

        while (running){
            int randNum = random.nextInt(1000);
            ctx.collect(new IntPOJO(
                    randNum,
                    randNum,
                    randNum,
                    1,
                    randNum
            ));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
