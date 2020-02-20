package java8._动态代理;

/**
 * @author lj
 * @createDate 2020/2/17 13:44
 **/
public class TestStaticProxy {

    public static void main(String[] args) {
        IRegisterService iRegisterService = new RegisterServiceImpl();
        IRegisterService proxy = new RegisterServiceProxy(iRegisterService);
        proxy.register("RyanLee", "123");
    }
}

interface IRegisterService {
    void register(String name, String pwd);
}

class RegisterServiceImpl implements IRegisterService {
    @Override
    public void register(String name, String pwd) {
        System.out.println(String.format("【向数据库中插入数据】name：%s，pwd：%s", name, pwd));
    }
}

class RegisterServiceProxy implements IRegisterService {
    IRegisterService iRegisterService;

    public RegisterServiceProxy(IRegisterService iRegisterService) {
        this.iRegisterService = iRegisterService;
    }

    @Override
    public void register(String name, String pwd) {
        System.out.println("[Proxy]一些前置处理");
        System.out.println(String.format("[Proxy]打印注册信息：姓名：%s,密码：%s", name, pwd));
        iRegisterService.register(name, pwd);
        System.out.println("[Proxy]一些后置处理");

    }
}

