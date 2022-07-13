package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @ClassName: HDFSClient_3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月13日 15时57分
 * @Version: v1.0
 */
public class HDFSClient_3 {
    // TODO review hdfs api
    public static FileSystem fs;
    @Before
    public void before() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname","true");
         fs = FileSystem.get(URI.create("hdfs://hadoop102:8020"), conf, "atguigu");
    }

    @Test
    public void test() throws IOException {
        fs.copyFromLocalFile(false,new Path("d:/111.txt"),new Path("/fan111.txt"));
    }
    @After
    public void close() throws IOException {
        fs.close();
    }
}
