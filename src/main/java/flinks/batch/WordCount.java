package flinks.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> data = env.readTextFile("D:\\idea\\project\\flinkstudy\\data\\test.txt");
        DataSet<String> source = data.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] split = value.split("\\s");
            for (String str : split) {
                out.collect(str);
            }
        }).returns(Types.STRING);
        MapOperator<String, Tuple2<String, Integer>> map = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });
        AggregateOperator<Tuple2<String, Integer>> sum = map.groupBy(0).sum(1);
        sum.print();

        //env.execute("aaaa");


        System.out.println(env.getExecutionPlan());

    }
}
