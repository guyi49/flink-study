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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * 功能 : 使用MapReduceAPI实现排序
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-08-04 08:51
 * @package : com.guyi.hadoop.mapreduce
 * @email : 853779011@qq.com
 */
public class SortApp {
    /**
     * 自定义MyMapper
     */
    private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException,InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data,new IntWritable(1));
        }
    }
    /**
     * 自定义MyReducer
     */
    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private static IntWritable data = new IntWritable(1);
        @Override
        protected void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            for (IntWritable val : values) {
                context.write(data,key);
                data = new IntWritable(data.get()+1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 定义输入，输出路径
        String INPUT_PATH = "/user/impala/gobs/input/sort";
        String OUTPUT_PATH = "/user/impala/gobs/output/outputSort";

        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        // 创建job
        Job job = Job.getInstance(conf,"SortApp");

        // 运行jar类
        job.setJarByClass(SortApp.class);

        // 设置map
        job.setMapperClass(MyMapper.class);

        // 设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入和输出目录
        FileInputFormat.setInputPaths(job,INPUT_PATH);
        job.setInputFormatClass(RecordReaderApp.MyInputFormat.class);

        // 设置输出格式
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);

        // 提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
