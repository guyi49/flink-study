package com.guyi.hadoop.hdfs.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * 功能 : SequenceFile 写操作
 * 步骤：
 * 1. 设置Configuration
 * 2. 获取FileSystem
 * 3. 设置文件输出格式
 * 4. SequenceFile.createWriter()创建SequenceFile.Write写入
 * 5. 调用SequenceFile.Write.append追加写入
 * 6. 关闭流
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-28 18:49
 * @package : com.guyi.hadoop.hdfs
 * @email : 853779011@qq.com
 */

public class SequenceFileWriter {
    private static Configuration configuration = new Configuration();
    private static String url = "";
    private static String[] data = {"a,s,d,f,g","q,w,e,r,t,y,u","z,x,c,v,b","p,l,o,k,m,i,j,n"};

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create(url),configuration);
        Path outputPath = new Path("MySequenceFile");
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem,configuration,outputPath,IntWritable.class,
                Text.class);
        for (int i = 0; i < 10; i++) {
            key.set(10-i);
            value.set(data[i%data.length]);
            writer.append(key,value);
        }
        IOUtils.closeStream(writer);
    }
}
