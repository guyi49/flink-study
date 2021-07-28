package com.guyi.hadoop.hdfs.mapfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;

/**
 * 功能 : MapFile 读操作
 * 步骤：
 * 1. 设置Configuration
 * 2. 获取FileSystem
 * 3. 设置文件输出路径
 * 4. MapFile.Reader()创建读取类MapFile.Reader
 * 5. 获取key和value的class
 * 6. 读取
 * 7. 关闭流
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-28 19:39
 * @package : com.guyi.hadoop.hdfs.mapfile
 * @email : 853779011@qq.com
 */
public class MapFileReader {
    private static Configuration configuration = new Configuration();
    private static String url = "";

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create(url),configuration);
        Path inputPath = new Path("MySequenceFile.seq");
        MapFile.Reader reader = new MapFile.Reader(fileSystem,inputPath.toString(),configuration);
        Writable keyClass = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),configuration);
        Writable valueClass = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),configuration);

        while (reader.next((WritableComparable) keyClass,valueClass)){
            System.out.println("key: " + keyClass);
            System.out.println("value: " + valueClass);
        }
        IOUtils.closeStream(reader);
    }
}
