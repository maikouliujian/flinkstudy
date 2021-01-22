package com.mine.study;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

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
public class Kafka2hive {

    //https://blog.csdn.net/weixin_41608066/article/details/110093474
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
        // hive
        // temp.ods_3rd_small_online_click
        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.useCatalog("default_catalog");
        //注册source和sink
        tEnv.executeSql(sourceDDL);



        //for hive
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/admin/IdeaProjects/mine/flinkstudy/flink12/src/main/resources"; // hive配置文件地址
        String version = "1.1.0";
        Catalog catalog = new HiveCatalog(name,defaultDatabase, hiveConfDir, version);
        tEnv.registerCatalog("myhive", catalog);
        tEnv.useCatalog("myhive");

        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.useDatabase("temp");

//        String[] strings = tEnv.listTables();
//        for (int i = 0; i < strings.length; i++) {
//            System.out.println(strings[i]);
//        }

        //数据提取
        //Table sourceTab = tEnv.from("kafka_source");
        //sourceTab.printSchema();
        //tEnv.executeSql("select * from default_catalog.default_database.kafka_source").print();

        //这里我们暂时先使用 标注了 deprecated 的API, 因为新的异步提交测试有待改进...
        //sourceTab.executeInsert("mysql_sink");
        //sourceTab.insertInto("mysql_sink");
        //执行作业
        String insertSql = "insert into  temp_kafka2hive SELECT " +
                "task,userid,`date`,'','',''," +
                "from_unixtime(UNIX_TIMESTAMP(`date`,'yyyy-MM-dd hh:mm:ss'),'yyyy-MM-dd') as `p_day`,\n" +
                "from_unixtime(UNIX_TIMESTAMP(`date`,'yyyy-MM-dd hh:mm:ss'),'HH') as `p_hour`" +
                "FROM default_catalog.default_database.kafka_source";
        tEnv.executeSql(insertSql);

        tEnv.execute("Flink Hello World");
    }
}