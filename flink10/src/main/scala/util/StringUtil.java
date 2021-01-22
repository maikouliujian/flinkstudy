package util;

/**
 * @author lj
 * @createDate 2019/12/27 11:36
 **/
public class StringUtil {

    public static String getRandomString(int len){
        StringBuilder sb = new StringBuilder();
        char tmp;
        for(int i=0;  i< len; i++){
            if(MathUtil.random.nextBoolean()){
                tmp = (char)(MathUtil.random.nextInt(26) + 65);
            }else{
                tmp = (char)(MathUtil.random.nextInt(26) + 97);
            }
            sb.append(tmp);
        }
        return sb.toString();
    }

    public static String getRandomString(){
        StringBuilder sb = new StringBuilder();
        char tmp;
        for(int i=0;  i<= 10; i++){
            if(MathUtil.random.nextBoolean()){
                tmp = (char)(MathUtil.random.nextInt(26) + 65);
            }else{
                tmp = (char)(MathUtil.random.nextInt(26) + 97);
            }
            sb.append(tmp);
        }
        return sb.toString();
    }

}
