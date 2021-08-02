package com.guyi.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 功能 : WordCount中只用Combiner
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-08-02 18:02
 * @package : com.guyi.hadoop.mapreduce
 * @email : 853779011@qq.com
 */
public class WordCountCombinerApp {
    public static class TokenizerMapper extends Mapper<Object, Text,Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        // map method
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable, Text,IntWritable> {
        private IntWritable result = new IntWritable();
        // reducer
        public void reduce(Text key, Iterable<IntWritable> values,Context context)
                throws IOException,InterruptedException{
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws Exception{

        // 定义配置类
        Configuration conf = new Configuration();

        // 创建job
        Job job = Job.getInstance(conf,"WordCountCombinerApp");

        // 运行jar类
        job.setJarByClass(WordCountCombinerApp.class);

        // 设置map
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 通过job设置Combiner处理类，其逻辑式直接使用Reducer
        job.setCombinerClass(IntSumReducer.class);

        // 设置reduce
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入格式
        FileInputFormat.addInputPath(job,new Path(args[0]));

        // 设置输出格式
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
