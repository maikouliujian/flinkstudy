package java8;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Media.print;

public class Start {

    List<Shop> shops;

    @Before
    public void before() {
        shops = Arrays.asList(new Shop("淘宝"),
                new Shop("天猫"),
                new Shop("京东"),
                new Shop("亚马逊"),
                new Shop("afas"),
                new Shop("asf"),
                new Shop("dd"),
                new Shop("dasa"));

    }


    /**
     *采用顺序查询所有商店的方式实现的 findPrices 方法,查询每个商店里的 iphone666s
     */
    @Test
    public void test() {
        long start = System.nanoTime();

        //List<String> list = findPrice4("iphone666s");
        List<String> list = findPrice2("iphone666s");

        System.out.println(list);
        System.out.println("Done in "+(System.nanoTime()-start)/1_000_000+" ms");
    }


    /**
     * 使用定制的 Executor 配置 CompletableFuture
     *
     * @param product
     * @return
     */
    public List<String> findPrice4(String product) {

        //为“最优价格查询器”应用定制的执行器 Execotor
        Executor executor = Executors.newFixedThreadPool(Math.min(shops.size(), 100),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        //使用守护线程,使用这种方式不会组织程序的关停
                        thread.setDaemon(true);
                        return thread;
                    }
                }
        );

        //将执行器Execotor 作为第二个参数传递给 supplyAsync 工厂方法
        List<CompletableFuture<String>> futures = shops.stream()
                /*.map(shop -> CompletableFuture.supplyAsync(
                        () -> String.format("%s price is %.2f RMB",
                                shop.getName(),
                                shop.getPrice(product)), executor)
                )*/
                .map(shop -> CompletableFuture.supplyAsync(new Supplier<String>() {
                    @Override
                    public String get() {
                        return String.format("%s price is %.2f RMB",
                                shop.getName(),
                                shop.getPrice(product));
                    }
                },executor))
                .collect(toList());
        List<String> list = futures.stream()
                .map(CompletableFuture::join)
                .collect(toList());


        return list;
    }



    /**
     * 得到折扣商店信息(已经被解析过)
     */
    public List<String> findPrice1(String product){
        List<String> list = shops.stream()
                .map(discountShop -> discountShop.getPrice(product))
                .map(Quote::parse)
                .map(Discount::applyDiscount)
                .collect(toList());

        return list;
    }


    /**
     * 使用 CompletableFuture 实现 findPrices 方法
     */
    public List<String> findPrice2(String product) {
        //为“最优价格查询器”应用定制的执行器 Execotor
        Executor executor = Executors.newFixedThreadPool(Math.min(shops.size(), 100),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        //使用守护线程,使用这种方式不会阻止程序的关停
                        thread.setDaemon(true);
                        return thread;
                    }
                }
        );

        List<CompletableFuture<String>> futureList = shops.stream()
                .map(discountShop -> CompletableFuture.supplyAsync(
                        //异步方式取得商店中产品价格
                        () -> discountShop.getPrice(product), executor))
                .map(future -> future.thenApply(Quote::parse))
                .map(future -> future.thenCompose(
                        quote -> {  return  CompletableFuture.supplyAsync(
                                //使用另一个异步任务访问折扣服务
                                () -> Discount.applyDiscount(quote), executor
                        );}
                ))
                .collect(toList());


        /*List<CompletableFuture<String>> futureList = shops.flinks.stream()
                .map(discountShop -> CompletableFuture.supplyAsync(
                        //异步方式取得商店中产品价格
                        () -> discountShop.getPrice(product), executor))
                .map(future -> future.thenApply(new Function<String, Quote>() {
                    @Override
                    public Quote apply(String s) {
                        return Quote.parse(s);
                    }
                }))
                .map(future -> future.thenCompose(
                        new Function<Quote, CompletionStage<String>>() {
                            @Override
                            public CompletionStage<String> apply(Quote quote) {
                                return CompletableFuture.supplyAsync(
                                        //使用另一个异步任务访问折扣服务
                                        () -> Discount.applyDiscount(quote), executor
                                );
                            }
                        }
                ))
                .collect(toList());*/



        //等待流中所有future执行完毕,并提取各自的返回值.
        List<String> list = futureList.stream()
                //join想但与future中的get方法,只是不会抛出异常
                .map(CompletableFuture::join)
                .collect(toList());

        return list;


    }


    @Test
    public void aaa(){



        /*Function<String,Shop> b = Shop::new;
        Shop aaaa = b.apply("aaaa");

        Function<Integer,Integer> f = (x) -> x + 1;
        Function<Integer,Integer> g = (x) -> x * 2;


        Function<Integer, Integer> h = f.andThen(g);
        Function<Integer, Integer> k = f.compose(g);


        Integer resutlt = k.apply(1);
        System.out.println(resutlt);*/

       /* Stream<Stream<int[]>> result = IntStream.rangeClosed(1, 100).boxed()
                .map(a -> IntStream.rangeClosed(a, 100)
                        .filter(b -> Math.sqrt(a * a + b * b) % 1 == 0)
                        .mapToObj(b -> new int[]{a, b, (int) Math.sqrt(a * a + b * b)})


                );

        result.forEach(System.out::print);*/


        Stream<int[]> stream = IntStream.rangeClosed(1, 100).boxed()
                .flatMap(a -> IntStream.rangeClosed(a, 100)
                        .filter(b -> Math.sqrt(a * a + b * b) % 1 == 0)
                        .mapToObj(b -> new int[]{a, b, (int) Math.sqrt(a * a + b * b)})


                );

        stream.forEach(System.out::print);

    }


    @Test
    public void start(){
        long a = sequentialSum(5L);
        System.out.println(a);
    }


    public Long sequentialSum(Long n){
        return Stream.iterate(1L, i->i+1L)
                .limit(n)
                .parallel()
                .reduce(0L,Long::sum);
    }



    @Test
    public void test9(){
        long[] numbers = LongStream.rangeClosed(1, 1000*10000).toArray();
        ForkJoinTest forkJoinTest = new ForkJoinTest(numbers);
        Long sum = new ForkJoinPool().invoke(forkJoinTest);
        System.out.println(sum);//50000005000000

    }
}
