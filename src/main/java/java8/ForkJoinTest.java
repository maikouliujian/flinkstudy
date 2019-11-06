package java8;

import java.util.concurrent.RecursiveTask;

/**
 * @author lj
 * @createDate 2019/7/22 16:59
 **/
public class ForkJoinTest extends RecursiveTask<Long> {
    //要处理的任务数组
    private final long[] numbers;

    //子任务处理数组的起始和结束位置
    private final int start;
    private final int end;

    //阀值,当数组大于10000就并行执行
    public static final long THRESHOLD = 10000;

    //公共构造函数,用于创建子任务


    //私有构造函数,用于 以递归方式为主任务创建子任务
    public ForkJoinTest(long[] numbers, int start, int end) {
        this.numbers = numbers;
        this.start = start;
        this.end = end;
    }

    public ForkJoinTest(long[] numbers) {
        this(numbers, 0, numbers.length);
    }


    @Override
    protected Long compute() {
        int length = end - start;

        //如果大小小于等于阀值,则顺序计算结果
        if (length <= THRESHOLD) {
            return computeSequentially();
        }

        //否则,创建一个子任务为数组的前一半求和
        ForkJoinTest leftTask = new ForkJoinTest(numbers, start, start + length / 2);
        //利用另一个 ForkJoinPool 里的线程异步执行新创建的子任务.
        leftTask.fork();//对子任务调用 fork 方法可以把它排进 ForkJoinPool 。
        //创建一个任务为数组的后一半求和
        ForkJoinTest rightTask = new ForkJoinTest(numbers, start + length / 2, end);
        //**递归操作**
        long rightResult = rightTask.compute();


        //遇到递归终止条件,读取本次递归第一个子任务的结果,如果尚未完成就等待
        long leftResult = leftTask.join();

        //递归累加
        return leftResult+rightResult;
    }

    //计算和
    private long computeSequentially() {

        long sum = 0;
        for (int i = start; i < end; i++) {

            sum += numbers[i];
        }
        return sum;
    }


}
