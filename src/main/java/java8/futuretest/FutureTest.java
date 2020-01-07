package java8.futuretest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author lj
 * @createDate 2019/12/31 15:47
 **/
public class FutureTest {

    public static void method() throws ExecutionException, InterruptedException {

        //第一个任务。
        CompletableFuture<String> f1 = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return "zhang";
            }
        });

        //第二个任务把第一个任务联合起来。
        CompletableFuture<String> f2 = f1.thenCombine(
                CompletableFuture.supplyAsync(new Supplier<String>() {
                    @Override
                    public String get() {
                        try {
                            TimeUnit.SECONDS.sleep(3);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        return "phil";
                    }
                }), new BiFunction<String, String, String>() {
                    @Override
                    public String apply(String s1, String s2) {
                        return s1 + s2;
                    }
                }
        );

        System.out.println("等待联合任务的全部执行完毕...");
        f2.whenCompleteAsync(new BiConsumer<String, Throwable>() {
            @Override
            public void accept(String s, Throwable throwable) {
                System.out.println("联合任务均完成");
                System.out.println(s);
            }
        });
        System.out.println("代码运行至此。");
    }

    public static void main(String[] args) throws Exception{
        method();
//        try {
//            method();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

    }
}
