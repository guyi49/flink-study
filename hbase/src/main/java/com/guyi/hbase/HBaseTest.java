package com.guyi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 功能 :
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-08-17 14:32
 * @package : com.guyi.hbase
 * @email : 853779011@qq.com
 */
public class HBaseTest {
    static Configuration cfg = HBaseConfiguration.create();

    // 列出数据库中所有表
    public static void list() throws IOException{
        // 创建数据库连接
        Connection conn = ConnectionFactory.createConnection(cfg);
        // Admin用于管理HBase数据库的表信息
        Admin admin = conn.getAdmin();
        System.out.println("=============== list tables: =============");
        for (TableName tn : admin.listTableNames()) {
            System.out.println(tn);
        }
        conn.close();
    }
    // 创建表
    public static void create(String tableName,String[]familyNames) throws IOException{
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        // HTableDescriptor包含了表的名字极其对应的列族
        HTableDescriptor htd = new HTableDescriptor(tn);
        for (String family:familyNames){
            htd.addFamily(new HColumnDescriptor(family));
        }
        admin.createTable(htd);
        conn.close();
        System.out.println("create table success!");
    }
    // 修改表——增加列族
    public static void addColumnFamily(String tableName,String[] familyNames) throws IOException{
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        // HTableDescriptor包含了表的名字极其对应的列族
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        for (String family:familyNames){
            htd.addFamily(new HColumnDescriptor(family));
        }
        admin.modifyTable(tn,htd);
        conn.close();
        System.out.println("modify success!");
    }
    // 修改表——减少列族
    public static void removeColumnFamily(String tableName,String[] familyNames) throws IOException{
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        // HTableDescriptor包含了表的名字极其对应的列族
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        for (String family:familyNames){
            htd.removeFamily(Bytes.toBytes(family));
        }
        admin.modifyTable(tn,htd);
        conn.close();
        System.out.println("remove success!");
    }
    // 查看表结构
    public static void describe(String tableName) throws IOException{
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        System.out.println("=========== describe " + tableName + ":============");
        for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
            System.out.println(hcd.getNameAsString());
        }
        System.out.println("===================================================");
        conn.close();
    }

    public static void main(String[] args) throws IOException{
//        create("scores", new String[]{"grade", "course"});
//        describe("scores");
//        addColumnFamily("scores",new String[]{"f1","f2"});
//        describe("scores");
//        removeColumnFamily("scores", new String[]{"f1"});
//        describe("scores");
        list();
    }
}
