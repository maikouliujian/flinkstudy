package java8.callback;

import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

/***
 * 类名::方法名

 注意是方法名哦，后面没有括号“()”哒。为啥不要括号，因为这样的是式子并不代表一定会调用这个方法。
 这种式子一般是用作Lambda表达式，Lambda有所谓懒加载嘛，不要括号就是说，看情况调用方法。

 例如

 表达式:

 person -> person.getAge();

 可以替换成

 Person::getAge

 表达式

 () -> new HashMap<>();

 可以替换成

 HashMap::new

 这种[方法引用]或者说[双冒号运算]对应的参数类型是Function<T,R> T表示传入类型，R表示返回类型。
 比如表达式person -> person.getAge(); 传入参数是person，返回值是person.getAge()，那么方法引用Person::getAge就对应着Function<Person,Integer>类型。
 ————————————————
 版权声明：本文为CSDN博主「lumence」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
 原文链接：https://blog.csdn.net/lsmsrc/article/details/41747159
 */
class Test {
    B b = new B();

    private void onProcessingTime(long time) throws Exception {

    }

    private void onTime(long time) throws Exception {

    }
    private void test(){
        //b.registerTimer(100L,this::onProcessingTime);
        //原始写法
        b.registerTimer(100L, new ProcessingTimeCallback() {
            @Override
            public void onProcessingTime(long timestamp) throws Exception {

            }
        });
        //TODO lambda表达式写法，只需要关注接口定义的函数即可，不需要关注接口类
        b.registerTimer(100L,timestamp -> onProcessingTime(timestamp));

        //TODO :进一步简化===>回调方法的定义，onProcessingTime方法的参数，在具体调用时来指定！！！
        //TODO ::这种[方法引用]
        b.registerTimer(100L,this::onProcessingTime);
        //TODO ::这种方式,方法的定义：方法名和接口定义不一样都可以，也就是方法名不重要，重要的是参数类型和返回值保持一致就可以
        b.registerTimer(100L,this::onTime);
    }
}


class B {
    public void registerTimer(long timestamp, ProcessingTimeCallback target) {

    }
}

