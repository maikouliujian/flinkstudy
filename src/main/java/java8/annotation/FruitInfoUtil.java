package java8.annotation;

import java.lang.reflect.Field;

/**
 * @author lj
 * @createDate 2020/1/16 15:07
 **/
///3：*********** 注解处理器 ***************/
public class FruitInfoUtil {

    public static void getFruitInfo(Class<?> clazz) {
        String strFruitProvicer = "供应商信息：";

        Field[] fields = clazz.getDeclaredFields();//通过反射获取处理注解
        for (Field field : fields) {
            if (field.isAnnotationPresent(FruitProvider.class)) {
                FruitProvider fruitProvider = (FruitProvider) field.getAnnotation(FruitProvider.class);
                //注解信息的处理地方
                strFruitProvicer = " 供应商编号：" + fruitProvider.id() + " 供应商名称："
                        + fruitProvider.name() + " 供应商地址："+ fruitProvider.address();
                System.out.println(strFruitProvicer);
            }
        }
    }
}
