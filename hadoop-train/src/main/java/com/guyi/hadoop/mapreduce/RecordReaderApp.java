package com.guyi.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

/**
 * 功能 : 计算奇数行与偶数行的数据之和
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-08-03 16:07
 * @package : com.guyi.hadoop.mapreduce
 * @email : 853779011@qq.com
 */
public class RecordReaderApp {
    public static class MyRecordReader extends RecordReader<LongWritable,Text>{
        // 起始位置（相对于整个分片而言）
        private long start;
        // 结束位置（相对于整个分片而言）
        private long end;
        // 当前位置
        private long pos;
        //文件的输入流
        private FSDataInputStream fin = null;
        // key,value
        private LongWritable key = null;
        private Text value = null;
        // 定义行阅读器
        private LineReader reader =null;

        // 初始化
        public void initialize(InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            // 获取分片
            FileSplit fileSplit = (FileSplit) inputSplit;
            // 获取起始位置
            start = fileSplit.getStart();
            // 获取结束位置
            end = fileSplit.getLength() + start;
            // 创建配置
            Configuration conf = context.getConfiguration();
            // 获取文件路径
            Path path = fileSplit.getPath();
            // 根据路径获取文件系统
            FileSystem fileSystem = path.getFileSystem(conf);
            // 打开文件输入流
            fin = fileSystem.open(path);
            // 找到开始位置开始读取
            fin.seek(start);
            // 创建阅读器
            reader = new LineReader(fin);
            // 将当前位置设置为1
            pos = 1;
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key ==null){
                key = new LongWritable();
            }
            key.set(pos);
            if (value ==null){
                value = new Text();
            }
            if (reader.readLine(value) ==0){
                return false;
            }
            pos ++;
            return true;
        }

        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        public void close() throws IOException {
            fin.close();
        }
    }

    /**
     * 自定义InputFormat
     */
    public class MyInputFormat extends FileInputFormat<LongWritable,Text>{

        public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            // 返回自定义对的RecordReader

            return new RecordReaderApp.MyRecordReader();
        }
        protected boolean isSplitable(FileSystem fs, Path fileName){
            return false;
        }
    }
    /**
     * 自定义MyPartitioner
     */
    public static class MyPartitioner extends Partitioner<LongWritable,Text> {
        // 转达给4个不同的reducer
        @Override
        public int getPartition(LongWritable key, Text value, int numPartition) {
            // 偶数放到第二个分区进行计算
            if (key.get() % 2 ==0){
                // 将输入到reduce的key设置为1
                key.set(1);
                return 1;
            }
            else{
                // 将输入到reduce的key设置为0
                key.set(0);
                return 0;
            }
        }
    }

    /**
     * 自定义MyMapper
     */
    private static class MyMapper extends Mapper<LongWritable, Text,LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,LongWritable,Text>.Context
                context) throws IOException,InterruptedException {
            // 直接将读取的记录写出去
            context.write(key,value);
        }
    }
    /**
     * 自定义MyReducer
     */
    public static class MyReducer extends Reducer<LongWritable,Text,Text,LongWritable>{
        // 创建写出去的key，value
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();
        @Override
        protected void reduce(LongWritable key,Iterable<Text> values,Reducer<LongWritable,Text,Text,
                LongWritable>.Context context) throws IOException,InterruptedException{
            System.out.println("奇数行还是偶数行： " + key);
            // 定义求和的变量
            int sum = 0;
            // 遍历value求和
            for (Text val : values) {
                // 累加
                sum += Long.parseLong(val.toString());
            }
            // 判断奇偶数
            if (key.get() == 0){
                outKey.set("奇数之和为： ");
            }else {
                outKey.set("偶数之和为： ");
            }
            // 设置value
            outValue.set(sum);
            // 把结果写出去
            context.write(outKey,outValue);
        }
    }
    // driver
    public static void main(String[] args) throws Exception {
        // 定义输入，输出路径
        String INPUT_PATH = "/user/impala/gobs/input/recordReader";
        String OUTPUT_PATH = "/user/impala/gobs/output/outputRecordReader";

        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        // 创建job
        Job job = Job.getInstance(conf,"RecordReaderApp");

        // 运行jar类
        job.setJarByClass(RecordReaderApp.class);

        // 设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置Partitioner
        job.setPartitionerClass(MyPartitioner.class);
        // 设置4个Reducer，每个分区一个
        job.setNumReduceTasks(2);

        // 设置输入目录和输入数据格式化的类
        FileInputFormat.setInputPaths(job,INPUT_PATH);
        job.setInputFormatClass(MyInputFormat.class);

        // 设置输出格式
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);

        // 提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
