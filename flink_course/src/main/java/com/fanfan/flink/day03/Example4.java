package com.fanfan.flink.day03;

import com.fanfan.flink.utils.ClickEvent;
import com.fanfan.flink.utils.ClickSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 10时47分
 * @Version: v1.0
 * TODO 手写Flink的Mysql连接器---Flink的NySQL驱动
 * 自定义输出
 * 实现幂等性写入~~
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                // 将数据源的数据写入到MySQL中
                .addSink(new MyJDBC());

        env.execute();
    }

    // RichSinkFunction是一个抽象类
    public static class MyJDBC extends RichSinkFunction<ClickEvent> {
        private Connection connection;
        private PreparedStatement insertStmt; // 插入语句
        private PreparedStatement updateStmt; // 更新语句

        // 初始化Mysql连接 与更新插入语句
        @Override
        public void open(Configuration parameters) throws Exception {


            // getConnection获取一个数据库的连接
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/userbehavior?useSSL=false",
                    "root",
                    "fanjingtao88"
            );
            insertStmt = connection.prepareStatement("insert into clicks (username, url) values(?,?)");
            updateStmt = connection.prepareStatement("update clicks set url = ? where username = ?");
        }

        // 每来一条数据调用一次
        @Override
        public void invoke(ClickEvent in, Context context) throws Exception {

            // 幂等性的写入mysql，每个username只有一条数据

            // 先进行更新
            updateStmt.setString(1, in.url);
            updateStmt.setString(2, in.username);
            updateStmt.execute();

            // 如果更新的行数为0,说明in.username对应的数据在表中不存在
            // 才插入数据，这样就保证幂等性写入了

            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, in.username);
                insertStmt.setString(2, in.url);
                insertStmt.execute();
            }
            System.out.println("数据更新成功：" + "用户名" + in.username + "url" + in.url);
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
