package flinks.transformation;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flinks.stream.MySource;

import java.util.ArrayList;
import java.util.List;

public class MyTask {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new MySource());

        SplitStream<Long> split = source.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> list = new ArrayList<>();
                if (value % 2 == 0) {
                    list.add("one");
                } else {
                    list.add("two");
                }
                return list;
            }
        });

        DataStream<Long> one = split.select("one");

//        one.shuffle()
//        one.rebalance()
        one.print();


        /*DataStreamSource<Long> source2 = env.addSource(new MySource());

        SingleOutputStreamOperator<String> source_text = source2.map((MapFunction<Long, String>) value -> "str_" + value).returns(Types.STRING);

        ConnectedStreams<Long, String> connect = source.connect(source_text);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Long, String, String>() {
            @Override
            public String map1(Long value) throws Exception {
                return "str_" + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });
        SingleOutputStreamOperator<String> returns = map.map((MapFunction<String, String>) value -> {
            System.out.println("接收到的数据为" + value);
            return value;
        }).returns(Types.STRING);

        SingleOutputStreamOperator<String> sum = returns.timeWindowAll(Time.seconds(2)).reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {
                return value1 + value2;
            }
        });
        sum.print().setParallelism(1);*/


        //默认checkpoint功能是disabled的，想要使用的时候需要先启用

// 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
// 高级选项：
// 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
// 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        env.execute("task");




    }
}