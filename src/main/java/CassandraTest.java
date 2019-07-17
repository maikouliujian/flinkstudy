import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.util.Collector;

/**
 * @author lj
 * @createDate 2019/6/27 14:18
 **/
public class CassandraTest {

    /*public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

// parse the data, group it, window it, and aggregate the counts
        DataStream<Tuple2<String, Long>> result = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                        // normalize and split the line
                        String[] words = value.toLowerCase().split("\\s");

                        // emit the pairs
                        for (String word : words) {
                            //Do not accept empty word, since word is defined as primary key in C* table
                            if (!word.isEmpty()) {
                                out.collect(new Tuple2<String, Long>(word, 1L));
                            }
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        CassandraSink.addSink(result)
                .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
                .setHost("127.0.0.1")
                .build();


        *//*DataSet<CustomCassandraAnnotatedPojo> inputDS = env
                .createInput(new CassandraPojoInputFormat<>(
                        SELECT_QUERY,
                        clusterBuilder,
                        CustomCassandraAnnotatedPojo.class,
                        () -> new Mapper.Option[]{Mapper.Option.consistencyLevel(ConsistencyLevel.ANY)}
                ));

        inputDS.print();*//*
    }*/
}
