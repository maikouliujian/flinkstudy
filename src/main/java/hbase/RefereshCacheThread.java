package hbase;

/**
 * Created by yangyibo
 * Date: 2019/5/5
 * Time: 下午5:18
 */
public class RefereshCacheThread implements Runnable {

	@Override
	public  void run(){

			try {
			    BlackListCache instance = BlackListCacheUtil.getInstance();
				instance.refreshCache();
			}catch (Exception e){
				e.printStackTrace();
			}
	}

}
