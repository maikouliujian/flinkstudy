package java8._动态代理;

/**
 * @author lj
 * @createDate 2020/2/17 13:48
 *
 *
 * https://www.cnblogs.com/biaogejiushibiao/p/9466097.html
 **/
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TestJdkDynamicProxy {

    public static void main(String[] args) {
        IRegisterService iRegisterService = new RegisterServiceImpl();
        InsertDataHandler insertDataHandler = new InsertDataHandler();
        IRegisterService proxy = (IRegisterService)insertDataHandler.getProxy(iRegisterService);
        proxy.register("RyanLee", "123");
    }
}


class InsertDataHandler implements InvocationHandler {
    Object obj;

    public Object getProxy(Object obj){
        this.obj = obj;
        return Proxy.newProxyInstance(obj.getClass().getClassLoader(), obj.getClass().getInterfaces(), this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        doBefore();
        Object result = method.invoke(obj, args);
        doAfter();
        return result;
    }
    private void doBefore() {
        System.out.println("[Proxy]一些前置处理");
    }
    private void doAfter() {
        System.out.println("[Proxy]一些后置处理");
    }

}

