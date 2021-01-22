import hbase.BlackListCache;
import hbase.BlackListCacheUtil;
import hbase.HBaseReader;
import hbase.UserId;
import hbase.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;


/**
 * Created by yangyibo
 * Date: 2019/5/5
 * Time: 下午8:39
 */


public class HbaseReaderTest {

	static HBaseReader hBaseReader;

	static {

		Configuration configuration = HBaseConfiguration.create();
//		configuration.set("hbase.zookeeper.quorum", "c3-d11-136-39-2.yidian.com,c3-d11-136-39-3.yidian.com");
//		configuration.set("hbase.zookeeper.property.clientPort", "2181");
//		configuration.set("zookeeper.znode.parent", "/hbase-olap");


		configuration.set("hbase.zookeeper.quorum","103-8-205-sh-100-F07.yidian.com,103-8-206-sh-100-F07.yidian.com,103-8-204-sh-100-F07.yidian.com");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("zookeeper.znode.parent", "/hbase");
		configuration.set("htable","spam_user_v2");

		hBaseReader=new HBaseReader(configuration);
	}


	@Test
	public void createTableTest(){
		hBaseReader.createTable("spam_user_v2");
	}

	public static List<String> readTxtFileIntoStringArrList(String filePath)
	{
		List<String> list = new ArrayList<String>();
		try
		{
			String encoding = "utf-8";
			File file = new File(filePath);
			if (file.isFile() && file.exists())
			{ // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(
						new FileInputStream(file), encoding);// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;

				while ((lineTxt = bufferedReader.readLine()) != null)
				{
					list.add(lineTxt);
				}
				bufferedReader.close();
				read.close();
			}
			else
			{
				System.out.println("找不到指定的文件");
			}
		}
		catch (Exception e)
		{
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}

		return list;
	}


	@Test
	public void deleteSomeData(){

		List<String> list = readTxtFileIntoStringArrList("/Users/admin/Downloads/spam.txt");
		long timeStamp=System.currentTimeMillis();
		for(int i=0;i<4;i++){
			List<UserId> userIds=new ArrayList<>(1000);
			for(int start=i*1000;start<(i+1)*1000;start++){
				userIds.add(new UserId(list.get(start),timeStamp,UserId.DELETE));
			}
			System.out.println(i);
			hBaseReader.store(userIds);

		}

	}


	@Test
	public void insertSomeData(){

		List<String> list = readTxtFileIntoStringArrList("/Users/admin/Downloads/spam.txt");
		long timeStamp=System.currentTimeMillis();
		for(int i=0;i<10;i++){
			List<UserId> userIds=new ArrayList<>(1000);
			for(int start=i*1000;start<(i+1)*1000;start++){
				userIds.add(new UserId(list.get(start),timeStamp,UserId.ADD));
			}
			System.out.println(i);
			hBaseReader.store(userIds);

		}
	}


	@Test
	public void  insetAllBlackListData(){
		List<String> list = readTxtFileIntoStringArrList("/Users/admin/Downloads/spam.txt");
		int chunkSize=10000;
		int chunkNum=list.size()/chunkSize+1;


		long timeStamp=System.currentTimeMillis();
		for(int i=0;i<chunkNum;i++){
			List<UserId> userIds=new ArrayList<>(chunkSize);
			for(int start=i*chunkSize;start<(i+1)*chunkSize && start<list.size();start++){
				userIds.add(new UserId(list.get(start),timeStamp));
			}
			System.out.println(i);
			hBaseReader.store(userIds);

		}

	}

	/**
	 * 这个方法是用来将redis的数据导入到hbase中
	 */
	@Test
	public  void loadRedisToHbase(){

		Jedis jedis=null;
		try{
			jedis = new Jedis("10.136.35.4", 6379, 5000);
			jedis.select(12);
			Set<String> spamUser = jedis.smembers("cjv-app-spam");
			List<String> list = new ArrayList(spamUser);
			int chunkSize=10000;
			int chunkNum=list.size()/chunkSize+1;

			System.out.println(spamUser.size());

			long timeStamp=System.currentTimeMillis();
			for(int i=0;i<chunkNum;i++){
				List<UserId> userIds=new ArrayList<>(chunkSize);
				for(int start=i*chunkSize;start<(i+1)*chunkSize && start<list.size();start++){
					userIds.add(new UserId(list.get(start),timeStamp));
				}
				System.out.println(i);
				hBaseReader.store(userIds);

			}



		}catch (Exception e){
			e.printStackTrace();
			//LOG.error("redis error "+e.getMessage());

		}finally {
			if(jedis!=null)
				jedis.close();
		}
	}




	@Test
	public void getOldDataTest(){


		long t=System.currentTimeMillis();
		int i=31;


			Set<UserId> oldData = hBaseReader.getData(t-i*24*3600*1000l,t-(i-1)*24*3600*1000l);

			System.out.println("day before "+i+" "+oldData.size());
		long currentTimeMillis = System.currentTimeMillis();

		System.out.println((currentTimeMillis-t)/1000l);




//		for(com.yidian.blacklist.UserId u:oldData){
//			System.out.println(u.getUserId());
//		}

	}

	@Test
	public void LoadHbaseToHdfs(){

		long t=System.currentTimeMillis();
		Set<UserId> oldData = hBaseReader.getData(t-30*24*3600*1000l,t);

	}



	@Test
	public  void getLatestData(){
		long start=System.currentTimeMillis();
		long t=start;
		Set<UserId> oldData = hBaseReader.getData(t-30*24*3600*1000l,t);
		System.out.println(oldData.size());


		long end=System.currentTimeMillis();
		System.out.println((end-start)/1000l);

	}



	@Test
	public void deleteData(){

		List<UserId> userIds=new ArrayList<>();
		userIds.add(new UserId("658016076",System.currentTimeMillis(),UserId.DELETE));
		hBaseReader.store(userIds);

	}


	@Test
	public void timeTest(){

		long start=System.currentTimeMillis();
		int sum=0;
		Random r=new Random(2);
		for(int i=0;i<830000;i++){
			sum+=i*(r.nextInt(4)+1);
		}
		long end=System.currentTimeMillis();
		System.out.println((end-start)/1000l +"   "+sum);
	}


	@Test
	public void test(){
		try{
			long start=System.currentTimeMillis();
			BlackListCache instance = BlackListCacheUtil.getInstance();
			while(true){
				instance.refreshCache();
				Set<String> cache = instance.getCache();
				System.out.println(cache.size() );
				Thread.sleep(30000l);
			}



		}catch (Exception e){
			e.printStackTrace();
		}

	}

	@Test
	public  void testTran(){
		UserId u=new UserId("yang",System.currentTimeMillis(),UserId.DELETE);
		byte[] bytes = Util.userIdToByte(u);
		UserId userId = Util.byteToUserId(bytes);
		System.out.println(userId.getUserId()+ " "+userId.getStatus());

	}

	@Test
	public  void testInsert(){
		BlackListCache instance = BlackListCacheUtil.getInstance();
		//instance.refreshCache();
		Set<String> cache = instance.getCache();
		//long userIdInsertTime = instance.getUserIdInsertTime("-1422742235", 1609470671000L, 1610507471000L);
		long userIdInsertTime = instance.getInsertTime("-1352865368", 1610212271000L, 1610644271000L);
		//System.out.println(cache.size());//122427
		System.out.println(userIdInsertTime);//122427
		//-1128918727
		//-2005121400
//        String[] ARR = new String[]{"-3022046319",
//                "-2756041158",
//                "-1723444916",
//                "-1193064250",
//                "-2931305362",
//                "-1380672151"};
//		String[] ARR = new String[]{"-1422742235"};
//		for (String s : ARR) {
//			if (cache.contains(s)){
//				System.out.println("black");
//			}else {
//				System.out.println("不是黑名单");
//			}
//		}

	}


}
