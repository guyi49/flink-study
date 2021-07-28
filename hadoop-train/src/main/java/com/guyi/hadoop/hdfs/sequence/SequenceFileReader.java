package com.guyi.hadoop.hdfs.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;

/**
 * 功能 : SequenceFile文件读操作
 * 步骤：
 * 1. 设置Configuration
 * 2. 获取FileSystem
 * 3. 设置文件输出路径
 * 4. SequenceFile.Reader()创建读取类SequenceFile.Reader
 * 5. 获取key和value的class
 * 6. 读取
 * 7. 关闭流
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-28 19:07
 * @package : com.guyi.hadoop.hdfs
 * @email : 853779011@qq.com
 */
public class SequenceFileReader {
    private static Configuration configuration = new Configuration();
    private static String url = "";
    private static String[] data = {"ashdj","qwertyu","zxcvb","pl,okmijn"};

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create(url),configuration);
        Path inputPath = new Path("MySequenceFile.seq");
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
