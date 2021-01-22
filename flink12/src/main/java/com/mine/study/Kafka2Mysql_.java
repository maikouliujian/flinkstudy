package com.mine.study;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - sql
 * 功能描述: 从Kafka读取消息数据，并在控制台进行打印。
 * 操作步骤:
 * 1. 先完成 Kafka2Print 的操作。
 * 2. 参考 订阅号文章 《Apache Flink 漫谈系列 - 搭建Flink 1.11 版本 Table API/SQL开发环境(需求驱动)》
 * https://mp.weixin.qq.com/s/Az8gqduAaQO-AD_MQaur7w 安装好MySQL环境，并创建 cdn_log 表。
 * 3. 启动作业，并参考文章说明，向Topic里面发送一些测试数据，然后在MySQL的shell命令行查询结果表。
 * <p>
 * 作者： 孙金城
 * 日期： 2020/8/2
 */
public class Kafka2Mysql_ {
    public static void main(String[] args) throws Exception {
        String kafka_brokers = "120-14-12-SH-1037-B05.yidian.com:9092, 120-14-11-SH-1037-B05.yidian.com:9092, 103-35-6-sh-100-E06.yidian.com:9092, 120-10-9-SH-1037-A05.yidian.com:9092, 103-35-5-sh-100-E06.yidian.com:9092, 120-10-5-SH-1037-A05.yidian.com:9092, 103-35-8-sh-100-E06.yidian.com:9092, 120-10-3-SH-1037-A05.yidian.com:9092, 120-10-6-SH-1037-A05.yidian.com:9092, 103-35-7-sh-100-E06.yidian.com:9092, 103-35-9-sh-100-E07.yidian.com:9092, 103-35-10-sh-100-E07.yidian.com:9092, 103-35-2-sh-100-E06.yidian.com:9092, 103-35-1-sh-100-E06.yidian.com:9092, 120-10-8-SH-1037-A05.yidian.com:9092, 103-35-4-sh-100-E06.yidian.com:9092, 120-10-7-SH-1037-A05.yidian.com:9092, 120-10-4-SH-1037-A05.yidian.com:9092, 103-35-3-sh-100-E06.yidian.com:9092, 120-10-19-SH-1037-A07.yidian.com:9092, 120-10-2-SH-1037-A05.yidian.com:9092";
        // Kafka {"msg": "welcome flink users..."}
        String sourceDDL = "CREATE TABLE kafka_source (\n" +
                " `date` STRING,\n" +
                " task STRING,\n" +
                " log STRING,\n" +
                " userid STRING,\n" +
                " ts as TO_TIMESTAMP(`date`),\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,\n" +
                " proctime as PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'indata_str_click_s3rd',\n" +
                " 'properties.bootstrap.servers' = '120-14-12-SH-1037-B05.yidian.com:9092, 120-14-11-SH-1037-B05.yidian.com:9092, 103-35-6-sh-100-E06.yidian.com:9092, 120-10-9-SH-1037-A05.yidian.com:9092, 103-35-5-sh-100-E06.yidian.com:9092, 120-10-5-SH-1037-A05.yidian.com:9092, 103-35-8-sh-100-E06.yidian.com:9092, 120-10-3-SH-1037-A05.yidian.com:9092, 120-10-6-SH-1037-A05.yidian.com:9092, 103-35-7-sh-100-E06.yidian.com:9092, 103-35-9-sh-100-E07.yidian.com:9092, 103-35-10-sh-100-E07.yidian.com:9092, 103-35-2-sh-100-E06.yidian.com:9092, 103-35-1-sh-100-E06.yidian.com:9092, 120-10-8-SH-1037-A05.yidian.com:9092, 103-35-4-sh-100-E06.yidian.com:9092, 120-10-7-SH-1037-A05.yidian.com:9092, 120-10-4-SH-1037-A05.yidian.com:9092, 103-35-3-sh-100-E06.yidian.com:9092, 120-10-19-SH-1037-A07.yidian.com:9092, 120-10-2-SH-1037-A05.yidian.com:9092',\n" +
                " 'properties.group.id' = 'flink12',\n"+
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset'\n" +
                ")";
        // Mysql
        String sinkDDL = "CREATE TABLE mysql_sink (\n" +
                " msg STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/flinkdb?characterEncoding=utf-8&useSSL=false',\n" +
                "   'table-name' = 'cdn_log',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'sink.buffer-flush.max-rows' = '1'\n" +
                ")";

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //注册source和sink
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        //数据提取
        //Table sourceTab = tEnv.from("kafka_source");
        //sourceTab.printSchema();
        tEnv.executeSql("select * from kafka_source").print();

        //这里我们暂时先使用 标注了 deprecated 的API, 因为新的异步提交测试有待改进...
        //sourceTab.executeInsert("mysql_sink");
        //sourceTab.insertInto("mysql_sink");
        //执行作业
        tEnv.execute("Flink Hello World");
    }
}