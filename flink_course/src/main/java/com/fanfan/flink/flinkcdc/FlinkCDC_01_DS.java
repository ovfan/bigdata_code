package com.fanfan.flink.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: FlinkCDC_01_DS
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月15日 13时58分
 * @Version: v1.0
 */
public class FlinkCDC_01_DS {
    public static void main(String[] args) throws Exception{
        // 1. 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //
        env.execute();
    }
}
