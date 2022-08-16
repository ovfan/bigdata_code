package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月16日 13时19分
 * @Version: v1.0
 */
public class Example2 {
    private static Connection connection;
    private static PreparedStatement insertStmt; // 插入语句
    public static void main(String[] args) throws SQLException {
        Properties info = new Properties();

        // 设置数据库 用户名及密码
        info.setProperty("user", "root");
        info.setProperty("password", "Bigdatafanfan0106");
        //获取连接
        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", info);

        // 准备插入语句
        insertStmt = connection.prepareStatement("insert into TableZ values(?, ?)");
        insertStmt.setInt(1,1);
        insertStmt.setInt(2, 40);

        // 执行
        insertStmt.execute();

        if (insertStmt.getUpdateCount() == 1) {
            System.out.println("数据插入成功");
        }

        insertStmt.close();
        connection.close();
    }
}
