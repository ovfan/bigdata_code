package com.fanfan.tutoria.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: Example6_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月20日 11时27分
 * @Version: v1.0
 */
public class Example6_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("192.168.44.102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, String>> out) throws Exception {
                        String[] words = in.split("\\s+");
                        if (words.length == 2) {
                            out.collect(Tuple2.of(words[0], words[1]));
                        }
                    }
                })
                .addSink(new RichSinkFunction<Tuple2<String, String>>() {
                    private Connection conn;
                    private PreparedStatement insertStmt;
                    private PreparedStatement updateStmt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/info.properties");
                        String user = parameterTool.get("u");
                        String password = parameterTool.get("p");

                        conn = DriverManager.getConnection("jdbc:mysql://192.168.44.102:3306/test?useSSL=false", user, password);
                        insertStmt = conn.prepareStatement("insert into clicks (username,url) values (?,?)");
                        updateStmt = conn.prepareStatement("update clicks set url = ? where username = ?");
                    }

                    @Override
                    public void invoke(Tuple2<String, String> in, Context ctx) throws Exception {
                        updateStmt.setString(1, in.f1);
                        updateStmt.setString(2, in.f0);
                        updateStmt.execute();

                        // 如果更新失败
                        if (updateStmt.getUpdateCount() == 0) {
                            insertStmt.setString(1, in.f0);
                            insertStmt.setString(2, in.f1);
                            insertStmt.execute();
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }
                }).setParallelism(4);

        env.execute();
    }
}
