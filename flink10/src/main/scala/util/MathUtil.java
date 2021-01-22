package util;

import java.util.Random;

/**
 * @author lj
 * @createDate 2019/12/27 11:36
 **/
public class MathUtil {

    public static Random random = new Random();
    public static int index =1;

    public static String getMediaCode(int i){
        String mediacode = fitNum(i);

        return mediacode;
    }

    private static String fitNum(int num){
        String str = String.valueOf(num);

        while (str.length() < 10){
            str = "0"+str;
        }
        return str;
    }

    public static String getRadomNum(int num){
        String tmp = "";
        for (int i =0; i< num; i++){
            tmp += random.nextInt(10);
        }

        return tmp;

    }
}
