package com.guyi.hadoop.io;

/**
 * 功能 :
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-27 15:29
 * @package : com.guyi.hadoop.io
 * @email : 853779011@qq.com
 */

import org.apache.hadoop.io.Writable;
import java.io.*;
import java.io.IOException;

/**
 * 序列化操作
 */
public class HadoopSerializationUtil {
    public static byte[] serialize(Writable writable) throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(out);
        writable.write(dataOutput);
        dataOutput.close();
        return out.toByteArray();
    }
    // 反序列化
    public static void deserialize(Writable writable, byte[] bytes) throws Exception{
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(in);
        writable.readFields(dataInputStream);
        dataInputStream.close();
    }

    public static void main(String[] args) throws Exception {
        Person person = new Person("gobs",23,"woman");
        byte[] values = HadoopSerializationUtil.serialize(person);
        // 测试反序列化
        Person p = new Person();
    }
}
