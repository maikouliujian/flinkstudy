package flinks.transformation;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.akka.org.jboss.netty.util.internal.ThreadLocalRandom;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flinks.stream.MySource;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CheckpointTask {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<Long> source = env.addSource(new MySource());
//
//        SplitStream<Long> split = source.split(new OutputSelector<Long>() {
//            @Override
//            public Iterable<String> select(Long value) {
//                List<String> list = new ArrayList<>();
//                if (value % 2 == 0) {
//                    list.add("one");
//                } else {
//                    list.add("two");
//                }
//                return list;
//            }
//        });
//
//        DataStream<Long> one = split.select("one");
//
////        one.shuffle()
////        one.rebalance()
//        one.print();


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

//// 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
//        env.enableCheckpointing(1000);
//// 高级选项：
//// 设置模式为exactly-once （这是默认值）
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//// 同一时间只允许进行一个检查点
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //map
//        Integer[] nums = new Integer[]{1, 2, 3, 4};
//        DataStream<Integer> dataStream = env.fromElements(nums);
//
//        dataStream.map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value * 2;
//            }
//        });

        //flatmap
//        String[] strings = new String[]{"i", "love", "flink"};
//        DataStream<String> dataStream = env.fromElements(strings);
//        dataStream.flatMap(new FlatMapFunction<String, String>() {
//
//            @Override
//            public void flatMap(String value, Collector<String> out) throws Exception {
//                out.collect(value);
//                char[] chs = value.toCharArray();
//                int i = 0, j = chs.length - 1;
//                while (i < j) {
//                    char tmp = chs[i];
//                    chs[i] = chs[j];
//                    chs[j] = tmp;
//                    i++;
//                    j--;
//                }
//                out.collect(new String(chs));
//            }
//        });

        //filter
        /*Integer[] nums = new Integer[]{1, 2, 3, 4};
        DataStream<Integer> dataStream = env.fromElements(nums);

        dataStream.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        });*/

        //process
//        String[] strings = new String[]{"i", "love", "flink"};
//        DataStream<String> dataStream = env.fromElements(strings);
//
//        OutputTag<String> outputTag = new OutputTag<String>("sideOut") {};
//
//        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStream.process(new ProcessFunction<String, String>() {
//            @Override
//            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
//                out.collect(value);
//                char[] chs = value.toCharArray();
//                int i = 0, j = chs.length - 1;
//                while (i < j) {
//                    char tmp = chs[i];
//                    chs[i] = chs[j];
//                    chs[j] = tmp;
//                    i++;
//                    j--;
//                }
//                ctx.output(outputTag, new String(chs));
//            }
//        });
//
//        stringSingleOutputStreamOperator.print();
//        stringSingleOutputStreamOperator.getSideOutput(outputTag).printToErr();

        //keyby
//        Integer[] integers = new Integer[]{1, 2, 3, 4,5,9};
//
//        DataStream<Integer> dataStream = env.fromElements(integers);
//        KeyedStream<Integer, Integer> keyedStream = dataStream.keyBy(new KeySelector<Integer, Integer>() {
//            @Override
//            public Integer getKey(Integer value) throws Exception {
//                return value % 2;
//            }
//        });
//        //keyedStream.printToErr();
//
//        keyedStream.sum(0).printToErr();

        //split 和 select

//        Integer[] integers = new Integer[]{1, 2, 3, 4};
////
////        DataStream<Integer> dataStream = env.fromElements(integers);
////
////        SplitStream<Integer> splitStream = dataStream.split(new OutputSelector<Integer>() {
////            @Override
////            public Iterable<String> select(Integer value) {
////                ArrayList<String> l = new ArrayList<>();
////                if (value % 2 == 0) {
////                    l.add("even");
////                } else {
////                    l.add("odd");
////                }
////                return l;
////            }
////        });
////
////        DataStream<Integer> selectStream = splitStream.select("even");
////
////        selectStream.printToErr();

        //project
//        Integer[] integers = new Integer[]{1, 2, 3, 4};
//
//        DataStream<Tuple4<Integer, Integer,Integer,Integer>> dataStream = env.fromElements(integers).
//                map(value -> Tuple4.of(value * 100, value,2,9)).
//                returns(Types.TUPLE(Types.INT,Types.INT,Types.INT,Types.INT));
//
//        dataStream.project(3,1).printToErr();
//
//        env.execute("task");

        //iterate
//        Integer[] integers = new Integer[]{1, 2, 3, 4};
//
//        env.setParallelism(1);
//
//        DataStream<Integer> dataStream = env.fromElements(integers);
//        IterativeStream<Integer> iterativeStream = dataStream.iterate(5000);
//
//        SplitStream<Integer> splitStream = iterativeStream.map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value * 2;
//            }
//        }).split(new OutputSelector<Integer>() {
//            @Override
//            public Iterable<String> select(Integer value) {
//                ArrayList<String> l = new ArrayList<>();
//                if (value > 100) {
//                    l.add("output");
//                } else {
//                    l.add("iterate");
//                }
//
//                return l;
//            }
//        });
//
//        iterativeStream.closeWith(splitStream.select("iterate"));
//        splitStream.select("output").printToErr();


        //TODO==============================keyedStream的操作==========================================
        //TODO=========================================================================================
        //reduce
//        String[] strings = new String[]{"i", "love", "flink"};
//
//        DataStream<String> dataStream = env.fromElements(strings);
//        dataStream.keyBy(new KeySelector<String, Byte>() {
//            @Override
//            public Byte getKey(String value) throws Exception {
//                return 0;
//            }
//        }).reduce(new ReduceFunction<String>() {
//            @Override
//            public String reduce(String value1, String value2) throws Exception {
//                return value1 + " " + value2;
//            }
//        }).printToErr();

        //fold
//        String[] strings = new String[]{"1", "2", "3"};
//
//        DataStream<String> dataStream = env.fromElements(strings);
//        dataStream.keyBy(new KeySelector<String, Byte>() {
//            @Override
//            public Byte getKey(String value) throws Exception {
//                return 0;
//            }
//        }).
//        fold(1, new FoldFunction<String, Integer>() {
//            @Override
//            public Integer fold(Integer accumulator, String value) throws Exception {
//                return accumulator + Integer.valueOf(value);
//            }
//
//        }).printToErr();

        //sum

//        Integer[] integers = new Integer[]{1, 2, 3};
//
//        DataStream<Integer> dataStream = env.fromElements(integers);
//
//        dataStream.keyBy(new KeySelector<Integer, Byte>() {
//
//            @Override
//            public Byte getKey(Integer value) throws Exception {
//                return 0;
//            }
//        }).sum(0).printToErr();

        //max/maxBy/min/minBy

//        Integer[] integers = new Integer[]{1, 2, 3};
//
//        DataStream<Integer> dataStream = env.fromElements(integers);
//
//        dataStream.keyBy(new KeySelector<Integer, Byte>() {
//
//            @Override
//            public Byte getKey(Integer value) throws Exception {
//                return 0;
//            }
//        }).max(0).printToErr();


//TODO==============================异步操作符==========================================
        Integer[] integers = new Integer[]{1, 2, 3, 4};

        DataStream<Integer> dataStream = env.fromElements(integers);

        AsyncDataStream.orderedWait(dataStream, new AsyncFunction<Integer, Integer>() {
            @Override
            public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
                Thread.sleep(ThreadLocalRandom.current().nextInt(5));
                ArrayList<Integer> l = new ArrayList<>();
                l.add(input);
                resultFuture.complete(l);
            }
        }, 10, TimeUnit.MILLISECONDS).printToErr();

        env.execute("task");




    }
}
