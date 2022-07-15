package com.fanfan.flink.day03_2;

import com.fanfan.flink.day03_1.ClickEvent;
import com.fanfan.flink.day03_1.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月15日 18时40分
 * @Version: v1.0
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 实现一个jdbc，对数据库进行幂等写入
        env
                .addSource(new ClickSource())
                .addSink(new MyJDBC());

        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<ClickEvent> {
        private Connection connection;
        private PreparedStatement updataStmt; // 更新语句
        private PreparedStatement insertStmt; // 插入语句

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取数据库的连接
            Properties info = new Properties();
            info.setProperty("user", "root");
            info.setProperty("password", "FanjingtaoMysql!@#88");
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/userbehavior?useSSL=false", info);

            // 准备更新语句与插入语句
            updataStmt = connection.prepareStatement("update clicks set url = ? where username = ?");
            insertStmt = connection.prepareStatement("insert into clicks values(?, ?)");
        }

        @Override
        public void invoke(ClickEvent in, Context ctx) throws Exception {
            // 每来一条数据调用一次
            // 首先更新操作
            updataStmt.setString(1, in.url);
            updataStmt.setString(2, in.user);
            // 不要忘记sql语句的执行
            updataStmt.execute();

            // 判断是否更新成功，如果未成功则插入数据
            if (updataStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, in.user);
                insertStmt.setString(2, in.url);
                insertStmt.execute();
            }
            System.out.println("数据更新成功：" + "用户名" + in.user + "url" + in.url);
        }

        @Override
        public void close() throws Exception {
            // 插入语句 与 更新语句 也要释放资源
            updataStmt.close();
            insertStmt.close();

            connection.close();
        }
    }
}
