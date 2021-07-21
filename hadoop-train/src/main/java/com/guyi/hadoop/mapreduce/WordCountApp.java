package com.guyi.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * 功能 :
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-21 14:34
 * @package : com.guyi.hadoop.mapreduce
 * @email : 853779011@qq.com
 */

/**
 * WordCount的MapReduce实现
 */
public class WordCountApp {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            //接收每一行数据
            String line = value.toString();
            //按空格进行分割
            String[] words = line.split(" ");
            for(String word :words){
                //通过上下文把map处理结果输出
                context.write(new Text(word), one);
            }
        }
    }
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values){
                //求单词次数
                sum += value.get();
            }
            //通过上下文把reduce处理结果输出
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        // 定义输入，输出路径
        String INPUT_PATH = "/user/impala/wc";
        String OUTPUT_PATH = "/user/impala/output";
        // 定义配置类
        Configuration conf = new Configuration();
        final FileSystem  fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job = Job.getInstance(conf,"WordCountApp");
        // 运行jar类
        job.setJarByClass(WordCountApp.class);
        // 设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        // 设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        // 设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(INPUT_PATH);
        FileInputFormat.addInputPath(job,inputPath);
        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outPath = new Path(OUTPUT_PATH);
        FileOutputFormat.setOutputPath(job,outPath);
        // 提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
