package com.fanfan.tutoria.dat02;

import org.apache.flink.util.MathUtils;

/**
 * @ClassName: Example6
 * @Description: keyby 路由到下游哪个并行子任务中
 * @Author: fanfan
 * @DateTime: 2022年08月19日 15时15分
 * @Version: v1.0
 */
public class Example6 {
    public static void main(String[] args) {
        // 计算key为0的数据要去的reduce的并行子任务的索引值
        Integer key = 0;
        // 获取key的hashcode
        int hashCode = key.hashCode();
        // 计算key的hashCode对应的murmurHash值
        int murmurHash = MathUtils.murmurHash(hashCode);

        // 128是默认的最大并行度，4是reduce算子的并行度
        // idx 是 key为0的数据将要路由到的reduce算子的并行子任务的索引
        // 第3个并行子任务的索引是2
        int idx = (murmurHash % 128) * 4 / 128;
        System.out.println("idx = " + idx);
        System.out.println("key为" + key + "的数据路由到下游reduce算子的第" + (idx + 1) + "个并行子任务中");
    }
}
