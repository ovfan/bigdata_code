package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @ClassName: HDFSClient_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 22时02分
 * @Version: v1.0
 */
public class HDFSClient_2 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop102:8020"), conf, "atguigu");

        // 测试文件上传
        fs.copyFromLocalFile(false, new Path("d:/111.txt"), new Path("/222.txt"));

        fs.close();
    }
}
