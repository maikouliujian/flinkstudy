package flinks.asynclo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Common;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author lj
 * @createDate 2019/12/27 17:07
 *  关于异步IO原理的讲解可以参考浪尖的知乎～：
 *     https://zhuanlan.zhihu.com/p/48686938
 **/
public class AsyncIOSideTableJoinRedis {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");
        properties.setProperty("group.id", "AsyncIOSideTableJoinRedis");

//        FlinkKafkaConsumer010<JSONObject> kafkaConsumer010 = new FlinkKafkaConsumer010<JSONObject>("jsontest",
//                new KafkaEventSchema(),
//                properties);

        FlinkKafkaConsumer<ObjectNode> source = new FlinkKafkaConsumer<>("async",
                new JsonNodeDeserializationSchema(), Common.getProp());

        DataStream<JSONObject> input = env.addSource(source).map(value -> {
            JSONObject jsonObject = new JSONObject(value);
            return jsonObject;
        });

        SampleAsyncFunction asyncFunction = new SampleAsyncFunction();

        // add async operator to streaming job
        DataStream<JSONObject> result;
        if (true) {
            result = AsyncDataStream.orderedWait(
                    input,
                    asyncFunction,
                    1000000L,
                    TimeUnit.MILLISECONDS,
                    20).setParallelism(1);
        }
        else {
            result = AsyncDataStream.unorderedWait(
                    input,
                    asyncFunction,
                    10000,
                    TimeUnit.MILLISECONDS,
                    20).setParallelism(1);
        }

        result.print();

        env.execute(AsyncIOSideTableJoinRedis.class.getCanonicalName());
    }

    private static class SampleAsyncFunction extends RichAsyncFunction<JSONObject, JSONObject> {
        private transient RedisClient redisClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            RedisOptions config = new RedisOptions();
            config.setHost("127.0.0.1");
            config.setPort(6379);

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);
            vo.setWorkerPoolSize(20);
            Vertx vertx = Vertx.vertx(vo);
            redisClient = RedisClient.create(vertx, config);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if(redisClient!=null)
                redisClient.close(null);

        }

        @Override
        public void asyncInvoke(final JSONObject input, final ResultFuture<JSONObject> resultFuture) {


            String fruit = input.getString("fruit");

            // 获取hash-key值
//            redisClient.hget(fruit,"hash-key",getRes->{
//            });
            // 直接通过key获取值，可以类比
            redisClient.get(fruit,getRes->{
                if(getRes.succeeded()){
                    String result = getRes.result();
                    if(result== null){
                        resultFuture.complete(null);
                        return;
                    }
                    else {
                        input.put("docs",result);
                        resultFuture.complete(Collections.singleton(input));
                    }
                } else if(getRes.failed()){
                    resultFuture.complete(null);
                    return;
                }
            });
        }

    }

}
