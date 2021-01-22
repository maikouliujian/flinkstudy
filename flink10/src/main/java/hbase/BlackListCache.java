package hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by yangyibo
 * Date: 2019/5/5
 * Time: 下午5:03
 */


public class BlackListCache  {



	private static final Log LOG = LogFactory.getLog(com.yidian.blacklist.BlackListCache.class);


	/**
	 *  blacklist cache
	 */
	private Set<String> blackListCache=new HashSet<>();
	private com.yidian.blacklist.HBaseReader hBaseReader;
	private volatile boolean deleted=false;
	/**
	 * 默认TTL设置为30 天
	 */
	private int TTL=30;
	private volatile long latestTimeStamp;


	BlackListCache(){

		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum","103-8-205-sh-100-F07.yidian.com,103-8-206-sh-100-F07.yidian.com,103-8-204-sh-100-F07.yidian.com");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("zookeeper.znode.parent", "/hbase");
		hBaseReader=new com.yidian.blacklist.HBaseReader(configuration);
		latestTimeStamp= System.currentTimeMillis();
	}



	/**
	 * 设置过期时间
	 * @param earlyDay
	 */
	public void setTTL(int earlyDay){
		TTL=earlyDay;
	}

	/**
	 * 获取全量blacklist cache
	 * @return
	 */

	public  Set<String> getCache(){
		synchronized(this){
			if(blackListCache.size()==0){
				refreshCache();
			}
			return blackListCache;
		}
	}

	/**
	 * 获取某个时间段的blackList
	 * @param startTimeStamp
	 * @param endTimeStamp
	 * @return
	 */
	public Set<String> getCache(long startTimeStamp,long endTimeStamp){
		Set<String> ret=new HashSet<>();
		Set<com.yidian.blacklist.UserId> data = hBaseReader.getData(startTimeStamp, endTimeStamp);
		for(com.yidian.blacklist.UserId u:data){
			ret.add(u.getUserId());
		}
		return  ret;
	}


    /****
	 * 我添加
	 * @param startTimeStamp
	 * @param endTimeStamp
	 * @return
	 */
	public long getInsertTime(String userId, long startTimeStamp,long endTimeStamp){
		Set<com.yidian.blacklist.UserId> data = hBaseReader.getData(startTimeStamp, endTimeStamp);
		System.out.println(data.size());
		for(com.yidian.blacklist.UserId u:data){
			//System.out.println(u.getUserId()+"----"+u.getTimetamp());
			if (userId.equals(u.getUserId())){
				return u.getTimetamp();
			}
		}
		return  -1L;
	}



	/**
	 * 刷新黑名单cache
	 */
	public void refreshCache(){

		synchronized (this){


			long currentTime=System.currentTimeMillis();

			// read new data from hbase
			long newStartTime=System.currentTimeMillis();
			Set<com.yidian.blacklist.UserId> newData=hBaseReader.getLatestData(TTL);

			//增加或者删除内存中的新数据
			for(com.yidian.blacklist.UserId t:newData){
				if(t.getStatus()== com.yidian.blacklist.UserId.DELETE){
					blackListCache.remove(t.getUserId());
					LOG.info("Delete remove user "+t.getUserId());
				}else if(t.getStatus()== com.yidian.blacklist.UserId.ADD){
					blackListCache.add(t.getUserId());
				}

			}


			long newendTime=System.currentTimeMillis();
			LOG.info("new read time "+(newendTime-newStartTime)+" ms "+ "size "+newData.size());


			if(!deleted){
				long oldStartTime=System.currentTimeMillis();
				// read  timeout  data from hbase
				Set<com.yidian.blacklist.UserId> oldData=hBaseReader.getData(currentTime-(TTL+1)*3600*24*1000L,currentTime-TTL*3600*24*1000L);
				//Set<UserId> oldData=hBaseReader.getData(currentTime-(TTL)*3600*24*1000L,currentTime-(TTL+10)*3600*24*1000L);

				// 删除内存中的数据
				for(com.yidian.blacklist.UserId t:oldData){
					blackListCache.remove(t.getUserId());
				}

				long olderendTime=System.currentTimeMillis();
				LOG.info("TTL remove "+(olderendTime-oldStartTime)+" ms" +" size "+oldData.size());

				//reset delete
				deleted=!deleted;
			}

			if((System.currentTimeMillis()-latestTimeStamp)>(24*3600*1000l)){
				latestTimeStamp=System.currentTimeMillis();
				deleted=false;
			}

//			if((System.currentTimeMillis()-latestTimeStamp)>(240*1000l)){
//				latestTimeStamp=System.currentTimeMillis();
//				deleted=false;
//			}



			long endTime=System.currentTimeMillis();
			LOG.info("total one time "+(endTime-currentTime)+" ms" +" new size "+blackListCache.size());
		}
	}

	/**
	 * 增加或者删除userId,通过设置status -1 or 1
	 * @param userIdList
	 */
	public void store(List<com.yidian.blacklist.UserId> userIdList){

		hBaseReader.store(userIdList);
	}

	/**
	 * 获取userId插入黑名单库的时间
	 * @param userId
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public long getUserIdInsertTime(String userId,long startTime, long endTime){

		if(startTime>endTime)
			return -1L;
		if(!getCache().contains(userId))
			return -1L;

		while(startTime<endTime){
			long mid=startTime+(endTime-startTime)/2;
			Set<String> cache = getCache(startTime, mid);
			if(cache.contains(userId)){
				endTime=mid;
			}else {
				startTime=mid;
			}

			if(endTime-startTime<1*60*1000L)
				break;

		}

		return startTime;
	}


}
