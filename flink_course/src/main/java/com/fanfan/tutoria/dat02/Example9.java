package com.fanfan.tutoria.dat02;

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
 * @ClassName: Example9
 * @Description: TODO 实现JDBC 连接器
 * @Author: fanfan
 * @DateTime: 2022年08月19日 18时13分
 * @Version: v1.0
 */
public class Example9 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取配置
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/info.properties");
        String user = parameterTool.get("user");
        String password = parameterTool.get("password");
        env
                .addSource(new ClickSource())
                .setParallelism(1)
                .addSink(new RichSinkFunction<ClickEvent>() {
                    private Connection conn;
                    private PreparedStatement insertStmt;
                    private PreparedStatement updateStmt;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false",
                                user,
                                password
                                );
                        insertStmt = conn.prepareStatement("insert into clicks values(?,?)");
                        updateStmt = conn.prepareStatement("update clicks set url = ? where username = ?");
                        System.out.println(conn);
                    }

                    // 实现幂等性写入 每个用户 只有最新一条浏览记录 存入数据库中
                    @Override
                    public void invoke(ClickEvent in, Context ctx) throws Exception {
                        updateStmt.setString(2,in.username);
                        updateStmt.setString(1,in.url);
                        // 不要忘记执行SQL
                        updateStmt.execute();
                        if(updateStmt.getUpdateCount() == 0){
                            // 更新失败，则插入数据
                            insertStmt.setString(1,in.username);
                            insertStmt.setString(2,in.url);
                            insertStmt.execute();
                        }
                    }
                    @Override
                    public void close() throws Exception {
                        insertStmt.close();
                        updateStmt.close();
                        conn.close();
                    }
                }).setParallelism(1);

        env.execute();
    }
}
