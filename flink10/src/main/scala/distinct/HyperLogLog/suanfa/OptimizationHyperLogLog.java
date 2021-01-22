package distinct.HyperLogLog.suanfa;

import java.util.HashSet;
import java.util.Set;

/***
 * 初始化：入参同样是一个允许的误差范围值rsd，计算出hyperloglog需要桶的个数bucket，也就需要是int数组大小，并且初始化一个set集合hashset;

 数据插入：使用与hyperloglog同样的方式将插入数据转hash, 判断当前集合的大小+1是否达到了bucket，不满足则直接添加到set中，
 满足则将set里面数据转移到hyperloglog对象中并且清空set, 后续数据将会被添加到hyperloglog中；

 这种写法没有考虑并发情况，在实际使用情况中也不会存在并发问题。
 */
public class OptimizationHyperLogLog {
    //hyperloglog结构
    private HyperLogLog hyperLogLog;
    //初始的一个set
    private Set<Integer> set;

    private double rsd;

    //hyperloglog的桶个数，主要内存占用
    private int bucket;

    public OptimizationHyperLogLog(double rsd){
        this.rsd=rsd;
        this.bucket=1 << HyperLogLog.log2m(rsd);
        set=new HashSet<>();
    }

    //插入一条数据
    public void offer(Object object){
        final int x = MurmurHash.hash(object);
        int currSize=set.size();
        if(hyperLogLog==null && currSize+1>bucket){
            //升级为hyperloglog
            hyperLogLog=new HyperLogLog(rsd);
            for(int d: set){
                hyperLogLog.offerHashed(d);
            }
            set.clear();
        }

        if(hyperLogLog!=null){
            hyperLogLog.offerHashed(x);
        }else {
            set.add(x);
        }
    }

    //获取大小
    public long cardinality() {
        if(hyperLogLog!=null) return hyperLogLog.cardinality();
        return set.size();
    }
}
