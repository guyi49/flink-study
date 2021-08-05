package com.guyi.hadoop.mapreduce;

import com.guyi.hadoop.pojo.IntPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 功能 : 使用MapReduceAPI 完成文件合并的功能
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-08-05 09:27
 * @package : com.guyi.hadoop.mapreduce
 * @email : 853779011@qq.com
 */
public class MergeApp {

    /**
     *  实现一个定制的RecordReader，6个方法皆继承RecordReader。
     */
    class WholeFileRecordReader extends RecordReader<NullWritable,BytesWritable>{
        private FileSplit fileSplit;
        private Configuration conf;
        private BytesWritable value = new BytesWritable();
        private boolean processed = false;

        public void initialize(InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) inputSplit;
            this.conf = context.getConfiguration();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed){
                byte[] contents = new byte[(int)fileSplit.getLength()];
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = null;
                try {
                    in = fs.open(file);
                    IOUtils.readFully(in,contents,0,contents.length);
                    value.set(contents,0,contents.length);
                }finally {
                    IOUtils.closeStream(in);
                }
                processed = true;
                return true;
            }
            return false;
        }

        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1.0f:0.0f;
        }

        public void close() throws IOException {
            // 啥都不干
        }
    }

    /**
     * 实现将整个文件作为一条记录处理的InputFormat
     */
    public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable>{
        // 设置每个小文件不可分片，保证一个小文件生成一个key-value
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            WholeFileRecordReader reader = new WholeFileRecordReader();
            reader.initialize(inputSplit,context);
            return reader;
        }
    }
    /**
     *  将小文件打包成SequenceFile
     */
    static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable, Text,BytesWritable> {
        private Text filenameKey;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            filenameKey = new Text(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(filenameKey,value);
        }
    }

    public static void main(String[] args) throws Exception {
        // 定义输入，输出路径
        String INPUT_PATH = "/user/impala/gobs/input/merge";
        String OUTPUT_PATH = "/user/impala/gobs/output/outputMerge";

        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        // 创建job
        Job job = Job.getInstance(conf,"MergeApp");

        // 运行jar类
        job.setJarByClass(MergeApp.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));

        // 分组函数
        job.setGroupingComparatorClass(SecondarySortApp.GroupingComparator.class);

        // 设置map
        job.setMapperClass(SequenceFileMapper.class);
//        job.setMapOutputKeyClass(IntPair.class);
//        job.setMapOutputValueClass(IntWritable.class);

        // 设置reduce
//        job.setReducerClass(SecondarySortApp.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置输入格式
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 设置输出格式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
