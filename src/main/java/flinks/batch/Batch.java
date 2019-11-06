package flinks.batch;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

/**
 * @author lj
 * @createDate 2019/6/27 16:45
 **/
public class Batch {

    //    private static final String INSERT_QUERY = "INSERT INTO test.batches (number, strings) VALUES (?,?);";
    private static final String SELECT_QUERY = "select uv, date  from sy3_data.v3_all_web_pv_uv_day;";

    /*
     *  table script: "CREATE TABLE test.batches (number int, strings text, PRIMARY KEY(number, strings));"
     */
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

//        ArrayList<Tuple2<Integer, String>> collection = new ArrayList<>(20);
//        for (int i = 0; i < 20; i++) {
//            collection.add(new Tuple2<>(i, "string " + i));
//        }
//
//        DataSet<Tuple2<Integer, String>> dataSet = env.fromCollection(collection);
//
//        dataSet.output(new CassandraTupleOutputFormat<Tuple2<Integer, String>>(INSERT_QUERY, new ClusterBuilder() {
//            @Override
//            protected Cluster buildCluster(Cluster.Builder builder) {
//                return builder.addContactPoints("127.0.0.1").build();
//            }
//        }));
//
//        env.execute("Write");

        DataSet<Tuple2<Integer, String>> inputDS = env
                .createInput(new CassandraInputFormat<Tuple2<Integer, String>>(SELECT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoints("172.18.0.40").build();
                    }
                }), TupleTypeInfo.of(new TypeHint<Tuple2<Integer, String>>() {
                }));
        /*inputDS.groupBy("date").reduce(new ReduceFunction<Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> reduce(Tuple2<Integer, String> integerStringTuple2, Tuple2<Integer, String> t1) throws Exception {
                return null;
            }
        })
*/
        env.execute("SELECT");
    }


}
