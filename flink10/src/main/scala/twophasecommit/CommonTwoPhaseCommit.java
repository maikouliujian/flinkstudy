package twophasecommit;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author lj
 * @createDate 2020/3/2 12:38
 * TODO 实现两阶段提交
 **/
public abstract class CommonTwoPhaseCommit<IN extends Serializable> extends RichSinkFunction<IN>

        implements CheckpointedFunction, CheckpointListener {

    private long checkpointId;
    private List<IN> dataList;
    private ListState<IN> dataListState;
    private ListState<Long> checkpointIdState;


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        dataList = new ArrayList<>();
        dataListState = context.getOperatorStateStore().getSerializableListState("listdata");
        checkpointIdState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("checkpointI", Long.class));
        if (context.isRestored()) {
            dataListState.get().forEach(x -> {
                dataList.add(x);
            });

            Iterator<Long> ckIdIter = checkpointIdState.get().iterator();
            checkpointId = ckIdIter.next();

            commit(dataList, checkpointId);

        }

    }


    @Override
    public void invoke(IN value, Context context) throws Exception {
        dataList.add(value);

    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        dataListState.clear();
        dataListState.addAll(dataList);
        dataList.clear();

        checkpointIdState.clear();
        checkpointId = context.getCheckpointId();
        checkpointIdState.addAll(Collections.singletonList(checkpointId));

    }


    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

        commit(dataListState.get(), checkpointId);

    }


    /**
     * 使用checkpoint与数据库已经存在值进行比较，要求正好比其大1
     *  目前该方案用于对window窗口聚合的延时补偿处理中，
     *  输出端为MySql，后期将会研究对Redis等其他数据库如何做一致性处理。
     * @param data
     * @param checkpointId
     */

    public abstract void commit(Iterable<IN> data, long checkpointId);
}
