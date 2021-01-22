package hbase;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yangyibo
 * Date: 2019/5/5
 * Time: 下午5:51
 */
public class Main {


	public  static  void main(String[] args){


		ScheduledExecutorService scheduledMetaLeaderExecutorService= new ScheduledThreadPoolExecutor(1,
				new NamedThreadFactoryService("refreshCache"));
		scheduledMetaLeaderExecutorService.scheduleAtFixedRate(new RefereshCacheThread(),0,30,TimeUnit.SECONDS);


	}
}
