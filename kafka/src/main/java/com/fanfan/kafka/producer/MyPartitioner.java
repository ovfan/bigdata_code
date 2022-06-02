package com.fanfan.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName: MyPartitioner
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月03日 00时21分
 * @Version: v1.0
 * 自定义分区器对象需要实现，kafaka提供的分区器接口
 * <p>
 * 需求：需求： 发送过来的数据中如果包含fan，就发往0号分区，不包含fan，就发往1号分区。
 */
public class MyPartitioner implements Partitioner {
    /**
     * 核心业务逻辑处理方法：（获取对应的分区编号）
     *
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 分区编号
        int partitionNum;
        // 获取消息内容
        String str = value.toString();
        if (str.contains("fan")) {
            partitionNum = 0;
        } else {
            partitionNum = 1;
        }
        return partitionNum;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {

    }

    /**
     * 获取当前客户端对象中的参数配置信息
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
            System.err.println(configs.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
    }
}
