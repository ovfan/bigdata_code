package com.fanfan.tutoria.day03;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: Example6
 * @Description: TODO sink to mysql
 * @Author: fanfan
 * @DateTime: 2022年08月20日 10时29分
 * @Version: v1.0
 */
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new ClickSource())
                .addSink(new MyJDBC());

        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<ClickEvent> {
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        // 获取数据库的连接
        @Override
        public void open(Configuration parameters) throws Exception {
            ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/info.properties");
            String user = parameterTool.get("u");
            String password = parameterTool.get("p");

            conn = DriverManager.getConnection("jdbc:mysql://192.168.44.102:3306/test?useSSL=false", user, password);
            insertStmt = conn.prepareStatement("insert into clicks (username,url) values (?,?)");
            updateStmt = conn.prepareStatement("update clicks set url = ? where username = ?");
        }

        // 每来一条数据，调用一次
        // 幂等性写入mysql
        @Override
        public void invoke(ClickEvent in, Context ctx) throws Exception {
            updateStmt.setString(1, in.url);
            updateStmt.setString(2, in.username);
            updateStmt.execute();

            // 如果更新失败
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, in.username);
                insertStmt.setString(2, in.url);
                insertStmt.execute();
            }
        }


        // 关闭数据库的连接
        @Override
        public void close() throws Exception {
            updateStmt.close();
            insertStmt.close();
            conn.close();
        }
    }
}
