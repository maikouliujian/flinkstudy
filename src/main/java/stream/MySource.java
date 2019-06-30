package stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/***
 * 并行度为1
 */
public class MySource implements SourceFunction<Long> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long count = 0;
        while (isRunning){
            ctx.collect(++count);
            Thread.sleep(1000);   //相当于1s发射一条数据
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
