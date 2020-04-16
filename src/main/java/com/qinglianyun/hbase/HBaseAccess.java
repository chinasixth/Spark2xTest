package com.qinglianyun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class HBaseAccess {
    private static Connection connection = null;
    private static Admin admin = null;

    public static void main(String[] args) {
        init();
        listTables();
    }

    private static void scanTable(String tableName) {
        TableName tb = TableName.valueOf(tableName);
        try {
            if (!admin.tableExists(tb)) {
                System.out.println("table is not exists");
            } else {
                Scan scan = new Scan();
                scan.withStartRow(Bytes.toBytes(""));
                scan.withStopRow(Bytes.toBytes(""));
                Table table = connection.getTable(tb);
                ResultScanner scanner = table.getScanner(scan);
                for (Result result : scanner) {
                    showCell(result);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    private static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println(cell.getTimestamp());
        }
    }

    private static void deleteTable(String tableName) {
        TableName tb = TableName.valueOf(tableName);
        try {
            if (!admin.tableExists(tb)) {
                System.out.println("table is not exits");
            } else {
                admin.deleteTable(tb);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param cols      列名
     * @return if create success return true
     */
    private static Boolean createTable(String tableName, String[] cols) {
        TableName tb = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tb)) {
                System.out.println("table is exists");
                return false;
            } else {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tb);
                for (String col : cols) {
                    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col));
                    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                }
                TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
                admin.createTable(tableDescriptor);
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return false;
    }

    /**
     * 列出所有表
     */
    private static void listTables() {
        try {
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for (TableDescriptor tableDescriptor : tableDescriptors) {
                System.out.println(tableDescriptor.getTableName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    /**
     * 初始化 HBase 连接
     */
    private static void init() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://ambari-test1:8020");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "ambari-test1,ambari-test2,ambari-test3");
        conf.set("hbase.master", "ambari-test1:16000");

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭 admin connection
     */
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
