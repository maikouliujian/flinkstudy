package hbase;

/**
 * Created by yangyibo
 * Date: 2019/5/5
 * Time: 下午5:29
 */
public class BlackListCacheUtil {


	private static  volatile BlackListCache blackListCache;




	public  static BlackListCache getInstance(){

		if(blackListCache==null){
			synchronized (BlackListCacheUtil.class){
				if(blackListCache==null){
					blackListCache=new BlackListCache();
				}
			}
		}

		return  blackListCache;
	}


}
