package com.fanfan.flink.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

/**
 * @ClassName: Example4_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 20时36分
 * @Version: v1.0
 * TODO 手写Flink的Mysql连接器---Flink的NySQL驱动
 * 需求；将自定义数据源的数据写入到mysql中
 * create table stu(id int,name varchar(20));
 * 将学生对象幂等写入Mysql中
 */
public class Example4_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new StuSource())
                .addSink(new MyJDBC());

        env.execute();
    }

    // Flink-SQL的连接器并将数据幂等写入
    public static class MyJDBC extends RichSinkFunction<Stu> {
        Connection conn = null;
        PreparedStatement insertStmt; // 插入语句
        PreparedStatement updateStmt; // 新增语句

        // 初始化数据库连接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "fanjingtao88");
            insertStmt = conn.prepareStatement("insert into stu (id, name) value (?,?)");
            updateStmt = conn.prepareStatement("update stu set name = ? where id = ?");
        }

        //实现幂等写入
        @Override
        public void invoke(Stu in, Context context) throws Exception {
            updateStmt.setString(1, in.name);
            updateStmt.setString(2, in.id);
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, in.id);
                insertStmt.setString(2, in.name);
                insertStmt.execute();
            }
            System.out.println("数据更新成功-id: " + in.id + " ,name: " + in.name);

        }

        @Override
        public void close() throws Exception {
            updateStmt.close();
            insertStmt.close();
            conn.close();
        }
    }

    public static class StuSource implements SourceFunction<Stu> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Stu> src) throws Exception {
            String[] idArr = {"1", "2", "3", "4", "5"};
            String[] userArr = {"fan", "qi", "tao", "peipei", "yueting", "qianqian"};
            while (running) {
                int randomIdIndex = random.nextInt(idArr.length);
                int randomUserIndex = random.nextInt(userArr.length);
                src.collect(new Stu(idArr[randomIdIndex], userArr[randomUserIndex]));

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Stu {
        public String id;
        public String name;

        public Stu() {
        }

        public Stu(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Stu{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    "}数据写入成功";
        }
    }
}
