package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @ClassName: HDFSClient_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 10时08分
 * @Version: v1.0
 */
public class HDFSClient_1 {
    FileSystem fs;
    @Before
    public void before() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 连接远程服务器需要加上这个配置项
        conf.set("dfs.client.use.datanode.hostname","true");
        fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),conf,"atguigu");
    }

    @Test
    public void test() throws IOException {
        // 创建文件夹
        fs.mkdirs(new Path("/test"));
    }

    @Test
    public void put() throws IOException {
        // 上传文件
        fs.copyFromLocalFile(false,new Path("d:/fan1.txt"),new Path("/test/"));
    }

    @Test
    public void get() throws IOException {
        // 下载文件
        fs.copyToLocalFile(false,new Path("/test/fan1.txt"),new Path("d:/fan2.txt"));
    }
    @After
    public void after() throws IOException {
        fs.close();
    }
}
