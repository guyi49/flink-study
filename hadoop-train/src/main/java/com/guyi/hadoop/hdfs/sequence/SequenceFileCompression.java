package com.guyi.hadoop.hdfs.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * 功能 : SequenceFile 压缩方式写操作
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-28 19:14
 * @package : com.guyi.hadoop.hdfs
 * @email : 853779011@qq.com
 */
public class SequenceFileCompression {
    static Configuration configuration = null;
    private static String url ="";

    static {
        configuration = new Configuration();
    }
    private static String[] data = {"a,s,d,f,g","q,w,e,r,t,y,u","z,x,c,v,b","p,l,o,k,m,i,j,n"};

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(url),configuration);
        Path outputPath = new Path("MySequenceFileCompression.seq");
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, configuration,outputPath,
                IntWritable.class, Text.class,SequenceFile.CompressionType.RECORD,new BZip2Codec());
        for (int i = 0; i < 10; i++) {
            key.set(10-i);
            value.set(data[i%data.length]);
            writer.append(key,value);
        }
        IOUtils.closeStream(writer);
        Path inputPath = new Path("MySequenceFileCompassion.seq");

        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem,inputPath,configuration);
        Writable keyClass = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),configuration);
        Writable valueClass = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),configuration);

        while (reader.next(keyClass,valueClass)){
            System.out.println("key: " + keyClass);
            System.out.println("value: " + valueClass);
            System.out.println("position: " + reader.getPosition());
        }
        IOUtils.closeStream(reader);
    }

}
