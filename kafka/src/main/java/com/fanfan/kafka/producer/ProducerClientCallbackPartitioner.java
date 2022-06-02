package com.fanfan.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName: ProducerClient
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月02日 22时33分
 * @Version: v1.0
 * 需求：验证Kafka的系统默认的分区规则&自定义分区规则
 * 默认的分区规则 and 自定义分区规则之间优先级
 * *       -- 自定义的优先级高
 * *       -- 注意：如果没有自定义，那默认规则中指定分区编号的操作优先级更高
 * <p>
 * 总结：
 * 1. 指定了分区编号，优先级最高
 * 2. 未指定分区编号，但指定了分区的key值，根据key的hascode值 % 分区数量，得出数据该发往哪个分区
 * 3. 未指定partition，也未封装key处理方式（采用粘性分区规则）
 * 参数1：producer发送的数据量：batch.size，默认值为16Kb；
 * 条件2：linger.ms：两条数据发送的间隔时间 t ，默认值为0s；
 * 当发送的数据量 < batch.size 并且 发送的数据时间间隔  < t   时，所有的数据在一个分区；
 * 当发送的数据量 > batch.size 或者 发送的数据时间间隔  >  t 时，则数据会进入下一个分区；
 * 分区与分区之间采取轮询的方式。
 */
public class ProducerClientCallbackPartitioner {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.202.102:9092,192.168.202.103:9092,192.168.202.103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 7);
        // properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        // 设置ack应答级别
//        properties.put(ProducerConfig.ACKS_CONFIG, "all");
//        properties.put(ProducerConfig.RETRIES_CONFIG, "5");
//        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // 开启幂等性

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", "fanfan1 我我我我我我我噢噢噢噢噢噢噢噢噢噢噢噢" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("主题:" + metadata.topic() + " 分区:" + metadata.partition() + "数据成功接受");
                    }
                }
            });
            //Thread.sleep(30);
        }
        // 不关闭资源，数据还没写出去，程序就结束了
        kafkaProducer.close();
    }
}
