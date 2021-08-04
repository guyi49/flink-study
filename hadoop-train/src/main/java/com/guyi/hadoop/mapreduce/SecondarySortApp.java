package com.guyi.hadoop.mapreduce;

import com.guyi.hadoop.pojo.IntPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import scala.Int;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * 功能 : 二次排序
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-08-04 09:28
 * @package : com.guyi.hadoop.mapreduce
 * @email : 853779011@qq.com
 */
public class SecondarySortApp {
    /**
     * 自定义MyMapper
     */
    private static class MyMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        private final IntPair key = new IntPair();
        private final IntWritable value = new IntWritable();
        @Override
        protected void map(LongWritable inKey, Text inValue,Context context)
                throws IOException,InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int left = 0;
            int right = 0;
            if (itr.hasMoreTokens()){
                left = Integer.parseInt(itr.nextToken());
                if (itr.hasMoreTokens()){
                    right = Integer.parseInt(itr.nextToken());
                }
                key.set(left,right);
                value.set(right);
                context.write(key,value);

            }
        }
    }

    /**
     * 在分组比较的时候，只比较原来的key，而是不是组合的key
     */
    public static class GroupingComparator implements RawComparator<IntPair>{

        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            return WritableComparator.compareBytes(bytes1,i,Integer.SIZE/8,bytes1,i2,Integer.SIZE/8);
        }

        public int compare(IntPair o1, IntPair o2) {
            int first1 = o1.getFirst();
            int first2 = o2.getFirst();
            return first1 - first2;
        }
    }
    /**
     * 自定义MyReducer
     */
    public static class MyReducer extends Reducer<IntPair,IntWritable,Text,IntWritable> {
        private static final Text SEPARATOR = new Text("----------------------");
        private final Text first = new Text();
        @Override
        protected void reduce(IntPair key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            context.write(SEPARATOR,null);
            first.set(Integer.toString(key.getFirst()));
            for (IntWritable val : values) {
                context.write(first,val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 定义输入，输出路径
        String INPUT_PATH = "/user/impala/gobs/input/secondarySort";
        String OUTPUT_PATH = "/user/impala/gobs/output/outputSecondarySort";

        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        // 创建job
        Job job = Job.getInstance(conf,"SecondarySortApp");

        // 运行jar类
        job.setJarByClass(SecondarySortApp.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));

        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        // 设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        // 提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
