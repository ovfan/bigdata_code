package com.fanfan.flink.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example2_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 20时17分
 * @Version: v1.0
 * TODO 使用自定义分区算子(partitionCustom) 将数据路由到指定的并行子任务中执行
 */
public class Example2_1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1,2,3,4,5,6,7,8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int i) {
                        if(key == 1){
                            return 0;
                        }
                        return 1;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        if(in % 2 == 0){
                            return 2;
                        }else{
                            return 1;
                        }

                    }
                })
                .print()
                .setParallelism(2);
        env.execute();
    }
}
