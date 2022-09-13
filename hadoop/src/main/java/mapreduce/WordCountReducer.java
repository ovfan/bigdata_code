package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: WordCountReducer
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年09月13日 20时32分
 * @Version: v1.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    private LongWritable acc = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        acc = new LongWritable(0);
    }

    /**
     * Reduce一次处理 一组数据，按组来处理数据，咱们需要将一组 1 处理成一个 n
     * 框架在调用的时候，一次处理的就是一个单词的所有的1
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Long acc = 0L;
        // 累加逻辑
        /*while (values.iterator().hasNext()) {
            this.acc.set(++acc);
        }*/
        acc = values.spliterator().getExactSizeIfKnown();
        this.acc.set(++acc);

        context.write(key, this.acc);
    }
}
