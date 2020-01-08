package java8.futuretest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author lj
 * @createDate 2019/12/31 16:09
 **/
public class Test {

    public static void main(String[] args)
    {
        int nThreads = 1;
        Executor e = Executors.newFixedThreadPool(nThreads);

        CompletableFuture.runAsync(() ->
        {
            System.out.println("Task 1. Thread: " + Thread.currentThread().getId());
        }, e).thenComposeAsync((Void unused) ->
        {
            return CompletableFuture.runAsync(() ->
            {
                System.out.println("Task 2. Thread: " + Thread.currentThread().getId());
            }, e);
        }, e).join();
        System.out.println("finished");
    }
}
