package com.guyi.hadoop.hdfs.mapfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.net.URI;

/**
 * 功能 : MapFile写文件
 * 步骤：
 * 1. 设置Configuration
 * 2. 获取FileSystem
 * 3. 设置文件输出格式
 * 4. MapFile.Write()创建MapFile.Write写入
 * 5. 调用MapFile.Write.append追加写入
 * 6. 关闭流
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-28 19:32
 * @package : com.guyi.hadoop.hdfs.mapfile
 * @email : 853779011@qq.com
 */
public class MapFileWriter {
    private static Configuration configuration = new Configuration();
    private static String url = "";

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create(url),configuration);
        Path outputPath = new Path("MyMapFile.map");

        Text key = new Text();
        key.set("myMapKey");
        Text value = new Text();
        value.set("myMapValue");

        MapFile.Writer writer = new MapFile.Writer(configuration,fileSystem,outputPath.toString(),
                Text.class, Text.class);
        writer.append(key,value);
        IOUtils.closeStream(writer);
    }
}
