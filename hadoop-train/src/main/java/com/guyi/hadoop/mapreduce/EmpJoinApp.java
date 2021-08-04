//package com.guyi.hadoop.mapreduce;
//
//import com.guyi.hadoop.entity.Emplyee;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.TimeoutException;
//
///**
// * @ProjectName: study-flink
// * @Package: com.guyi.hadoop.mapreduce
// * @ClassName: EmpJoinApp
// * @Author: GuYi
// * @email: 853779011@qq.com
// * @Description: 使用MapReduceAPI完成Reduce join开发
// * @Date: 2021/8/3
// */
//public class EmpJoinApp {
//    /**
//     * 自定义MyMapper
//     */
//    private static class MyMapper extends Mapper<LongWritable, Text,LongWritable, Emplyee> {
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
//            String val = value.toString();
//            String[] arr = val.split("\t");
//            System.out.println("arr.length = " + arr.length + " arr[0] " + arr[0]);
//            // dept
//            if (arr.length <=3){
//                Emplyee e = new Emplyee();
//                e.setDeptNo(arr[0]);
//                e.setDeptName(arr[1]);
//                e.setFlag(1);
//                context.write(new LongWritable(Long.parseLong(e.getDeptNo())),e);
//            }else {
//                Emplyee e = new Emplyee();
//                e.setEmpNo(arr[0]);
//                e.setEmpName(arr[1]);
//                e.setDeptNo(arr[7]);
//                e.setFlag(0);
//                context.write(new LongWritable(Long.parseLong(e.getDeptNo())),e);
//            }
//
//        }
//    }
//    /**
//     * 自定义MyReducer
//     */
//    public static class MyReducer extends Reducer<LongWritable,Emplyee, NullWritable, Text> {
//        // 创建写出去的key，value
//        private Text outKey = new Text();
//        private LongWritable outValue = new LongWritable();
//        @Override
//        protected void reduce(LongWritable key,Iterable<Emplyee> iter,Context context)
//                throws IOException,InterruptedException{
//            Emplyee dept = null;
//            List<Emplyee> list = new ArrayList<Emplyee>();
//
//            for (Emplyee tmp : iter) {
//                // emp
//                if (tmp.getFlag() == 0){
//                    Emplyee emplyee = new Emplyee(tmp);
//                    list.add(emplyee);
//                }else {
//                    dept = new Emplyee(tmp);
//
//                }
//            }
//            if (dept != null){
//                for (Emplyee emp : list) {
//                    emp.setDeptName(dept.getDeptName());
//                    context.write(NullWritable.get(),new Text(emp.toString()));
//                }
//            }
//        }
//    }
//    // driver
//    public static void main(String[] args) throws Exception {
//        // 定义输入，输出路径
//        String INPUT_PATH = "/user/impala/gobs/input/inputJoin";
//        String OUTPUT_PATH = "/user/impala/gobs/output/outputMapJoin";
//
//        Configuration conf = new Configuration();
//        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
//        if (fileSystem.exists(new Path(OUTPUT_PATH))){
//            fileSystem.delete(new Path(OUTPUT_PATH),true);
//        }
//        // 创建job
//        Job job = Job.getInstance(conf,"EmpJoinApp");
//
//        // 运行jar类
//        job.setJarByClass(EmpJoinApp.class);
//
//        // 设置map
//        job.setMapperClass(MyMapper.class);
//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(Emplyee.class);
//
//        // 设置reduce
//        job.setReducerClass(MyReducer.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Emplyee.class);
//
//
//        // 设置输入目录和输入数据格式化的类
//        FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
//        // 设置输出格式
//        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
//
//        // 提交job
//        System.exit(job.waitForCompletion(true)?0:1);
//    }
//}
