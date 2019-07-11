package batch;

import org.apache.commons.io.FileUtils;
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


import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DistributedCache {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //注册
        env.registerCachedFile("d:\\idea\\itcast.log","haha");

        DataSource<String> sources = env.fromElements("a", "b", "c");
        MapOperator<String, String> result = sources.map(new RichMapFunction<String, String>() {

            List<String> datas = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                //super.open(parameters);
                File haha = getRuntimeContext().getDistributedCache().getFile("haha");
                List<String> lines = FileUtils.readLines(haha);

                datas.addAll(lines);
                System.out.println(datas);
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        result.print();

        //env.execute("aaaa");

    }
}
