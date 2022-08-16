package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 13时53分
 * @Version: v1.0
 * JDBC:连接驱动 Java项目中需要单独引入进来，Maven项目中pom文件中引入即可
 *         <dependency>
 *             <groupId>mysql</groupId>
 *             <artifactId>mysql-connector-java</artifactId>
 *             <version>8.0.21</version>
 *         </dependency>
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        // 通过JDBC方式连接数据库
        // 1,获取数据库连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/userbehavior?useSSL=false&serverTimezone=UTC", "root", "fanjingtao88");
        System.out.println(conn);
    }
}
