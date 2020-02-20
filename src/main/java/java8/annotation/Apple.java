package java8.annotation;

/**
 * @author lj
 * @createDate 2020/1/16 15:05
 **/
public class Apple {


    @FruitProvider(id = 1, name = "陕西红富士集团", address = "陕西省西安市延安路")
    private String appleProvider;


    public void setAppleProvider(String appleProvider) {
        this.appleProvider = appleProvider;
    }
    public String getAppleProvider() {
        return appleProvider;
    }
}
