package com.fanfan.tutoria.day08;

import com.fanfan.tutoria.utils.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月27日 10时34分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置每隔10s保存一次检查点
        env.enableCheckpointing(10 * 1000L);
        // 设置保存检查点文件的路径
        env.setStateBackend(new FsStateBackend("file://" + "flink_course/src/main/resources/ckpts"));

        env
                .addSource(new ClickSource())
                .print();

        env.execute();
    }
}
