package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @ClassName: HDFSClient
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 09时32分
 * @Version: v1.0
 * TODO 使用hadoop client客户端提供的API实现文件的上传下载删除，创建文件夹功能
 */
public class HDFSClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        // API操作的套路

        Configuration conf = new Configuration();
        conf.setInt("dfs.replication",2);        // 设置副本数量为2
        // 如果使用远程云服务器需要添加此配置信息--才能成功写入文件(因为nn与dn是通过局域网通信的)
        conf.set("dfs.client.use.datanode.hostname", "true");

        // 1. 创建客户端对象(HDFS是文件系统，所以使用FileSystem)
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:8020"), conf, "atguigu");
        // 2. 使用这个对象的API方法完成指定功能

        // fileSystem.mkdirs(new Path("/test"));
        // 2.1 上传文件
        fileSystem.copyFromLocalFile(false,true,new Path("d:/fanfan.txt"),new Path("/"));

        // 2.2 下载文件
        fileSystem.copyToLocalFile(false,new Path("/fanfan.txt"),new Path("d:/fan1.txt"));

        // 2.3 删除文件
        fileSystem.delete(new Path("/fanfan.txt"),false);
        // 3. 关闭资源
        fileSystem.close();
    }
}
