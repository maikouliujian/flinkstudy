package flinks.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Iterator;

/**
 * @author : 刘剑
 * @date : 2020/11/13 2:44 下午
 * https://blog.csdn.net/lzb348110175/article/details/104356709
 * @description
 * ODO 自定义可并行Source(保证Exactly-Once)
 *  * --->extends RichParallelSourceFunction:用于实现可并行 Source
 *  * --->implements CheckpointedFunction：Checkpoint机制，用来保证 Exactly-Once
 */
public class MyExactlyOnceParFileSource extends RichParallelSourceFunction<Tuple2<String,String>> implements CheckpointedFunction {
    private String path = "C:\\Users\\a\\Desktop\\test";

    private Boolean flag = true;

    private long offset = 0;//偏移量默认值

    private transient ListState<Long> offsetState;//状态数据不参与序列化，添加 transient 修饰

    public MyExactlyOnceParFileSource() {
    }

    public MyExactlyOnceParFileSource(String path) {
        this.path = path;
    }

    /**
     * run()方法，用于一直运行产生数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        //获取 offsetState 中的历史值（赋值给offset）
        Iterator<Long> iterator = offsetState.get().iterator();
        while (iterator.hasNext()) {
            offset = iterator.next();
        }
        //获取当前 subTask 的 index 值
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        //定义用于读取的文件路径
        RandomAccessFile randomAccessFile = new RandomAccessFile(path+"/"+subtaskIndex+".txt", "r");

        //从指定偏移量位置读取
        randomAccessFile.seek(offset);

        //多并行线程不安全问题。需要加锁。其实 Flink 已经帮我们想到这种情况了。
        final Object checkpointLock = ctx.getCheckpointLock();//最好用final修饰

        while (flag) {
            String line = randomAccessFile.readLine();
            if (line != null) {
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");

                synchronized (checkpointLock){
                    //获取 randomAccessFile 已经读完数据的指针
                    offset = randomAccessFile.getFilePointer();
                    //将数据发送出去
                    ctx.collect(Tuple2.of(subtaskIndex+"",line));
                }
            }else{
                Thread.sleep(1000);
            }
        }

    }

    /**
     * cancel() 方法，用于关闭Source
     */
    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * 定期将指定的状态数据，保存到 StateBackEnd 中
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //将历史值清除
        offsetState.clear();

        // 更新最新的状态值
        offsetState.add(offset);

    }

    /**
     * 初始化状态(初始化OperatorState)  相当于subTask new完成之后构造器的生命周期方法，构造器执行完会执行一次
     *
     * 从 StateBackend 中取状态
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //定义一个状态描述器(根据状态描述器 1.初始化状态 或者 2.获取历史状态)
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<Long>(
                "offset-state",//指定状态描述器s称(可以随便定义，但是一个Job任务中不能重复)
                Types.LONG
//                TypeInformation.of(new TypeHint<Long>() {})
//                Long.class
        );
        //todo 获取 operatorState 数据
        offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
    }
}
