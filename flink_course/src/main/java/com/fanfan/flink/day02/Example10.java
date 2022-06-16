package com.fanfan.flink.day02;

import org.apache.flink.util.MathUtils;

/**
 * @ClassName: Example10
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 22时55分
 * @Version: v1.0
 */
public class Example10 {
    public static void main(String[] args) {
        //TODO keyBy的底层原理是Hash算法 之 murmurHash算法

        // 1. 计算Key的数据要取得reduce并行子任务的索引值
        Integer key = 2;
        //  获取key的hashCod
        int hashcode = key.hashCode();
        System.out.println("hashcode = " + hashcode);

        // 2. 计算key的hashcode值对应的murmurHash值
        int murmurHash = MathUtils.murmurHash(hashcode);
        System.out.println("murmurHash = " + murmurHash);

        // 3. 128是默认的最大并行度，4是reduce算子的并行度
        // idx是key为0的数据要路由到reduce的并行子任务的索引值 (从0开始)
        //  例如：第三个并行子任务的索引是2
        int idx = (murmurHash % 128) * 4 / 128;
        System.out.println("idx = " + idx);

    }
}
