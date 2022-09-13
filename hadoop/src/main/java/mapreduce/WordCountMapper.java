package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: WordCountMapper
 * @Description: TODO Mr版-WordCount
 * @Author: fanfan
 * @DateTime: 2022年09月13日 20时32分
 * @Version: v1.0
 */

/**
 * 泛型：<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * 框架给我们提供的输入数据的泛型，是固定的 LongWritable,Text :
 * 数据经过框架的预处理后，数据到达mapper的时候，框架已经把数据拆分成一行一行的数据了，给我们提供的是一行一行的数据来处理
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = null;
    private IntWritable one = null;

    // 在mapTask任务开始的时候调用一次setup
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        word = new Text();
        one = new IntWritable(1);
    }

    /**
     * TODO 我们写的代码 供框架来调用
     * 每行数据 调用一次map,将一行所有单词变成(word, 1) 的形式
     * key:行偏移量 value:一行的内容
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分一行内容为 单词
        String[] words = value.toString().split(" ");

        // 遍历所有单词并输出
        for (String word : words) {
            this.word.set(word);
            // 输出 -- context可以理解为Map阶段本身
            context.write(this.word, one);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
