package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BatchBroadcast {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1、准备广播数据
        List<Tuple2<String,Integer>> datas = new ArrayList<>();


        datas.add(new Tuple2<String,Integer>("zhangsan",5));
        datas.add(new Tuple2<String,Integer>("lisi",7));
        datas.add(new Tuple2<String,Integer>("wangwu",9));

        DataSource<Tuple2<String, Integer>> broadcasts = env.fromCollection(datas);

        //处理广播数据
        MapOperator<Tuple2<String, Integer>, HashMap<String, String>> broad = broadcasts.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, String>>() {


            @Override
            public HashMap<String, String> map(Tuple2<String, Integer> value) throws Exception {

                HashMap<String, String> re = new HashMap<>();
                re.put(value.f0, value.f1 + "");
                return re;
            }
        });

        DataSource<String> source = env.fromElements("zhangsan", "lisi", "wangwu");
        MapOperator<String, String> result = source.map(new RichMapFunction<String, String>() {

            List<HashMap<String, String>> broadcast = new ArrayList<>();
            HashMap<String, String> allmap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                //super.open(parameters);
                broadcast = getRuntimeContext().getBroadcastVariable("broadcast");
                for (HashMap<String, String> map : broadcast) {
                    allmap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                String age = allmap.get(value);
                return value + "----" + age;
            }
        }).withBroadcastSet(broad, "broadcast");


        result.print();


    }
}
