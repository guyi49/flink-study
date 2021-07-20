package hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
/**
 * 功能 :
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-07-20 16:28
 * @package : com.guyi.hadoop
 * @email : 853779011@qq.com
 */
public class HDFSAPP {
    // HDFS文件系统服务器的地址以及端口
    public static final String HDFS_PATH = "hdfs://192.168.77.130:8020";
    // HDFS文件系统的操作对象
    FileSystem fileSystem = null;
    // 配置对象
    Configuration configuration = null;

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir()throws Exception{
        // 需要传递一个Path对象
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    // 准备资源
    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        // 第一参数是服务器的URI，第二个参数是配置对象，第三个参数是文件系统的用户名
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
        System.out.println("HDFSAPP.setUp");
    }

    // 释放资源
    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSAPP.tearDown");
    }
}
