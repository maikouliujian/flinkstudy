package transformation;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import stream.MySource;

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

        env.execute("task");




    }
}
