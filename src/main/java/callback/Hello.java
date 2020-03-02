package callback;

/**
 * @author lj
 * @createDate 2020/3/2 19:09
 **/
public class Hello {
    public void run(CallBack callBack){
        callBack.test("id");
    }

    public void haha(String idx){
        System.out.println("call back" +idx);
    }
//    public void Hhhh(int idx){
//        System.out.println("call back" +idx);
//    }

    public void aa(){
        run(this::haha);
    }
}

class Start{
    public static void main(String[] args) {
        Hello h = new Hello();
        h.aa();
    }
}
