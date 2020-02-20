package flinks.sink;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author lj
 * @createDate 2020/1/21 17:44
 *
 * 见：https://blog.csdn.net/wangpei1949/article/details/103326461
 **/

@Slf4j
public class AppendStreamTableSinkTest {


    public static void main(String[] args) throws Exception{

        args=new String[]{"--application","flink/src/main/java/com/bigdata/flink/tableSqlAppendUpsertRetract/application.properties"};

        //1、解析命令行参数
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("application"));

        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");

        //2、设置运行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers",kafkaBootstrapServers);
        browseProperties.put("group.id",browseTopicGroupID);
        DataStream<UserBrowseLog> browseStream=streamEnv
                .addSource(new FlinkKafkaConsumer010<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction());
        tableEnv.registerDataStream("source_kafka_browse_log",browseStream,"userID,eventTime,eventType,productID,productPrice,eventTimeTimestamp");


        //4、注册AppendStreamTableSink
        String[] sinkFieldNames={"userID","eventTime","eventType","productID","productPrice","eventTimeTimestamp"};
        DataType[] sinkFieldTypes={DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.INT(),DataTypes.BIGINT()};
        TableSink<Row> myAppendStreamTableSink = new MyAppendStreamTableSink(sinkFieldNames,sinkFieldTypes);
        tableEnv.registerTableSink("sink_stdout",myAppendStreamTableSink);


        //5、连续查询
        //将userID为user_1的记录输出到外部存储
        String sql="insert into sink_stdout select userID,eventTime,eventType,productID,productPrice,eventTimeTimestamp " +
                "from source_kafka_browse_log where userID='user_1'";
        tableEnv.sqlUpdate(sql);

        //6、开始执行
        tableEnv.execute(AppendStreamTableSinkTest.class.getSimpleName());

        /***
         * // Table只有Insert操作，不需要特殊编码
         * user_1,2016-01-01 10:02:06,browse,product_5,20,1451613726000
         * user_1,2016-01-01 10:02:00,browse,product_5,20,1451613720000
         * user_1,2016-01-01 10:02:15,browse,product_5,20,1451613735000
         * user_1,2016-01-01 10:02:02,browse,product_5,20,1451613722000
         * user_1,2016-01-01 10:02:16,browse,product_5,20,1451613736000
         */

    }


    /**
     * 解析Kafka数据
     * 将Kafka JSON String 解析成JavaBean: UserBrowseLog
     * UserBrowseLog(String userID, String eventTime, String eventType, String productID, int productPrice, long eventTimeTimestamp)
     */
    private static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {

                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                java.time.format.DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getEventTime(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setEventTimeTimestamp(eventTimeTimestamp);

                out.collect(log);
            }catch (Exception ex){
                log.error("解析Kafka数据异常...",ex);
            }
        }
    }

    /**
     * 自定义 AppendStreamTableSink
     * AppendStreamTableSink 适用于表只有Insert的场景
     */
    private static class MyAppendStreamTableSink implements AppendStreamTableSink<Row> {

        private TableSchema tableSchema;

        public MyAppendStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
            this.tableSchema = TableSchema.builder().fields(fieldNames,fieldTypes).build();
        }

        @Override
        public TableSchema getTableSchema() {
            return tableSchema;
        }

        @Override
        public DataType getConsumedDataType() {
            return tableSchema.toRowDataType();
        }

        // 已过时
        @Override
        public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            return null;
        }

        // 已过时
        @Override
        public void emitDataStream(DataStream<Row> dataStream) {}

        @Override
        public DataStreamSink<Row> consumeDataStream(DataStream<Row> dataStream) {
            return dataStream.addSink(new SinkFunction());
        }

        private static class SinkFunction extends RichSinkFunction<Row> {
            public SinkFunction() {
            }

            @Override
            public void invoke(Row value, Context context) throws Exception {
                System.out.println(value);
            }
        }
    }
}
