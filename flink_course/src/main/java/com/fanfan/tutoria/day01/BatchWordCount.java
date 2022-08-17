package com.fanfan.tutoria.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName: BatchWordCount
 * @Description: TODO flink-批计算 wordcount
 *                  DataSet-API flink1.12以后 官方推荐使用 批流统一 API -- DataStream API
 * @Author: fanfan
 * @DateTime: 2022年08月17日 12时44分
 * @Version: v1.0
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境(批计算环境)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据 DataSource -> Operator -> DataSet 本质是数据集
        DataSource<String> lineDataSource = env.readTextFile("flink_course/src/main/resources/word.txt");

        // 3. 具体的处理逻辑-将每行数据进行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDataSource.flatMap((String in, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4. 将word进行分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroup = wordAndOne.groupBy(0);

        // 5. 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Integer>> wordCount = wordAndOneGroup.sum(1);

        // 6. 结果的打印输出
        wordCount.print();

    }
}
