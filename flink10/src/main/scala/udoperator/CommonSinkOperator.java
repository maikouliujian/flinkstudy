package udoperator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/***
 * # todo 需求：如何做定时输出，首先说一下定时输出的需求背景，在flink流处理中需要将任务处理的结果数据定时输出到外部存储中例如mysql/hbase等，
 * # todo 如果我们单条输出就可能会造成对外部存储造成较大的压力，首先我们想到的批量输出，就是当需要输出的数据累计到一定大小然后批量写入外部存储
 *
 *https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247483802&idx=1&sn=0fdb31192bbacd2062468ef7a050b540&chksm=fe2b65d5c95cecc39bf4fd92b2e5b798a52be30b0359fe95d84c8049ee952439831de13e179b&scene=21#wechat_redirect
 * https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247484057&idx=1&sn=3312590b9fd114704a660e6cd32ea139&chksm=fe2b66d6c95cefc09030c229176e6817a67acd08bc068ce98f77702a5b3bd9948478e884e9a7&scene=21#wechat_redirect
 * 如何调用？直接使用dataStream.transform方式即可。

 整体来说这个demo相对来说是比较简单的，但是这里面涉及的定时、状态管理也是值得研究，比喻说在这里定时我们直接选择ProcessingTimeService，
 而没有选择InternalTimerService来完成定时注册，主要是由于InternalTimerService会做定时调用状态保存，
 在窗口操作中需要任务失败重启仍然可以触发定时，但是在我们案例中不需要，直接下次启动重新注册即可，因此选择了ProcessingTimeService。

 定义一个CommonSinkOperator的抽象类，继承AbstractStreamOperator，
 并且实现ProcessingTimeCallback与OneInputStreamOperator接口，那么看下类里面的方法，

 1、首先看构造函数传入批写大小batchSize与定时写入时间interval

 2、重载了AbstractStreamOperator的open方法，在这个方法里面获取ProcessingTimeService，然后注册一个interval时长的定时器

 3、重载AbstractStreamOperator的initializeState方法，用于恢复内存数据

 4、重载AbstractStreamOperator的snapshotState方法，在checkpoint会将内存数据写入状态中容错

 5、实现了OneInputStreamOperator接口的processElement方法，将结果数据写入到内存中，如果满足一定大小则输出

 6、实现了ProcessingTimeCallback接口的onProcessingTime方法，注册定时器的执行方法，进行数据输出的同时，注册下一个定时器

 7、定义了一个抽象saveRecords方法，实际输出操作


 那么这个CommonSinkOperator就是一个模板方法，能够做到将任何类型的数据输出，只需要继承该类，并且实现saveRecords方法即可。
 * @param <T>
 */

public abstract class CommonSinkOperator<T extends Serializable> extends AbstractStreamOperator<Object>
        implements ProcessingTimeCallback,OneInputStreamOperator<T,Object> {

    private List<T> list;
    private ListState<T> listState;
    private int batchSize;
    private long interval;
    private ProcessingTimeService processingTimeService;

    public CommonSinkOperator(){

    }
    public CommonSinkOperator(int batchSize,long interval){
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = batchSize;
        this.interval = interval;

    }

    @Override
    public void open()throws Exception{
        super.open();
        if(interval >0&& batchSize >1){
//获取AbstractStreamOperator里面的ProcessingTimeService， 该对象用来做定时调用
//注册定时器将当前对象作为回调对象，需要实现ProcessingTimeCallback接口
            processingTimeService = getProcessingTimeService();
            long now = processingTimeService.getCurrentProcessingTime();
            processingTimeService.registerTimer(now + interval,this);
        }
    }

    //快照
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (list.size() > 0) {
            listState.clear();
            listState.addAll(list);
        }
    }


    //状态恢复
    @Override
    public void initializeState(StateInitializationContext context)throws Exception{
        super.initializeState(context);
        this.list =new ArrayList<T>();
        listState = context.getOperatorStateStore().getSerializableListState("batch-interval-sink");
        if(context.isRestored()){
            listState.get().forEach(x ->{
                list.add(x);
            });
        }
    }

    @Override
    public void processElement(StreamRecord<T> element)throws Exception {
        list.add(element.getValue());
        if (list.size() >= batchSize) {
            saveRecords(list);
        }
    }

    //定时回调
    @Override
    public void onProcessingTime(long timestamp)throws Exception{
        if(list.size()>0){
            saveRecords(list);
            list.clear();
        }
        long now = processingTimeService.getCurrentProcessingTime();
        processingTimeService.registerTimer(now + interval,this);//再次注册
    }

    public abstract void saveRecords(List<T> datas);

}
