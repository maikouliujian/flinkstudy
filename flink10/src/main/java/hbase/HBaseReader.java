package hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by yangyibo
 * Date: 2019/5/5
 * Time: 下午5:02
 */

public class HBaseReader {


	/**
	 * 上次读取的最新的timestamp
	 */


	private static final Log LOG = LogFactory.getLog(HBaseReader.class);

	private Connection connections;
	private String tableName;
	private long timeout;
	private Configuration conf;

	/* kindex table schema: $reverseTimestamp = cf1:bl:userId */
	static final String CF = "cf1";
	static final String USERID_QUALIFIER = "usr";
	private byte[] lastRowKey;





	public HBaseReader(Configuration conf)  {

		createConnection(conf);
		tableName = conf.get("htable","spam_user_v2");
		timeout = conf.getInt("hbase.timeout",30000);
		this.conf = conf;
	}




	public static String reverse(String str)
	{
		return new StringBuffer(str).reverse().toString();
	}


	public byte[] getRow(long timestamp, UserId userId) {

		byte[] bytes = Bytes.toBytes(userId.getUserId());
		long reverseTimestamp = Long.MAX_VALUE - timestamp;
		ByteBuffer buffer = ByteBuffer.allocate(bytes.length+ 8);
		buffer.putLong(reverseTimestamp);
		buffer.put(bytes);

		return buffer.array();

	}




	public void removeData(long startTimeStamp,long endTimeStamp){

		Table table = null;
		Scan scan = null;
		ResultScanner scanner = null;
		try {
			table = connections.getTable(TableName.valueOf(tableName));
			scan = new Scan();
			scan.setTimeRange(startTimeStamp, endTimeStamp);  // STOP_TS: The timestamp in question
           // Crucial optimization: Make sure you process multiple rows together
			scan.setCaching(10);
            // Crucial optimization: Retrieve only row keys
			FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL,
					new FirstKeyOnlyFilter(), new KeyOnlyFilter());
			scan.setFilter(filters);
			scanner = table.getScanner(scan);
			List<Delete> deletes = new ArrayList<>(10);
			Result[] rr;
			do {
				// We set caching to 1000 above
				// make full use of it and get next 1000 rows in one go
				rr = scanner.next(10);
				if (rr.length > 0) {
					for (Result r: rr) {
						Delete delete = new Delete(r.getRow(), endTimeStamp);
						deletes.add(delete);
					}
					table.delete(deletes);
					deletes.clear();
				}
			} while(rr.length > 0);
		} catch (Exception e) {
			LOG.error(e.getMessage());

		} finally {
			if(scanner!=null){
				scanner.close();
			}
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					LOG.error(e.getMessage());
				}
			}
		}


	}


	/**
	 *创建表
	 * @param tableName
	 */
	public  void  createTable(String tableName){

		try{


			Admin hBaseAdmin= connections.getAdmin();
			if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {// 如果存在要创建的表，那么先删除，再创建
				 hBaseAdmin.disableTable(TableName.valueOf(tableName));
				 hBaseAdmin.deleteTable(TableName.valueOf(tableName));
				 LOG.info(tableName + " is exist,detele....");
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		tableDescriptor.addFamily(new HColumnDescriptor(CF).setCompressionType(Compression.Algorithm.SNAPPY));
		hBaseAdmin.createTable(tableDescriptor);
		hBaseAdmin.close();
	} catch (MasterNotRunningException e) {
		e.printStackTrace();
	} catch (ZooKeeperConnectionException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}



}








	/**
	  存储userIds
	 */
	public void store(List<UserId> userIds){

		Table table = null;
		ResultScanner scanner = null;
		try {
			table = connections.getTable(TableName.valueOf(tableName));
			List<Put> puts=new ArrayList<>(userIds.size());

			for(UserId u:userIds){
				Put put=new Put(getRow(u.getTimetamp(),u));
				put.addColumn(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER), Util.userIdToByte(u));
				put.setDurability(Durability.ASYNC_WAL);
				puts.add(put);
			}

			table.put(puts);
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			if(scanner!=null){
				scanner.close();
			}
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}



	/**
	 * 获取最新的数据
	 * @return
	 */
	public Set<UserId> getLatestData(int ttl){

		Set<UserId> ret=new HashSet<>();

		Table table = null;
		Scan scan = null;
		ResultScanner scanner = null;
		Scan dataScan=null;
		try {
			table = connections.getTable(TableName.valueOf(tableName));
			scan = new Scan();
			scan.addColumn(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
			scan.setMaxResultSize(1);
			scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();

			if(it.hasNext()){
				Result next = it.next();
				byte[] rowKeyBytes = next.getRow();

				//第一次读
				if(lastRowKey==null){

					dataScan = new Scan();
					long startTimeStamp=System.currentTimeMillis()-ttl*3600*24*1000L;
					long endTimeStamp=System.currentTimeMillis();
					dataScan.addColumn(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
					dataScan.setTimeRange(startTimeStamp,endTimeStamp);
					ResultScanner dataScanner = table.getScanner(dataScan);
					Iterator<Result> ite = dataScanner.iterator();
					while (ite.hasNext()) {
						byte[] valueBytes = ite.next().getValue(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
						UserId userId = Util.byteToUserId(valueBytes);
						ret.add(userId);
					}
					lastRowKey=rowKeyBytes;


				}else{
					//后序开始读
					dataScan = new Scan();
					dataScan.addColumn(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
					dataScan.setStartRow(rowKeyBytes);
					dataScan.setStopRow(lastRowKey);
					ResultScanner dataScanner = table.getScanner(dataScan);
					Iterator<Result> ite = dataScanner.iterator();
					while (ite.hasNext()) {
						byte[] valueBytes = ite.next().getValue(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
						UserId userId = Util.byteToUserId(valueBytes);
						ret.add(userId);
					}
					lastRowKey=rowKeyBytes;

				}

			}

			return ret;
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			if(scanner!=null){
				scanner.close();
			}
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}


		return ret;

	}





	/**
	 * 获取过期的数据
	 * @param startTimeStamp
	 * @param endTimeStamp
	 * @return
	 */
	public Set<UserId> getData(Long startTimeStamp, long endTimeStamp){
		Set<UserId> ret=new HashSet<>();

		Table table = null;
		Scan scan = null;
		ResultScanner scanner = null;
		try {
			table = connections.getTable(TableName.valueOf(tableName));
			long start=System.currentTimeMillis();
			scan = new Scan();
			//scan.setStartRow(getRow(endTimeStamp));
			//scan.setStopRow(getRow(startTimeStamp));
			scan.addColumn(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
			scan.setTimeRange(startTimeStamp,endTimeStamp);
			scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			long end=System.currentTimeMillis();
			//LOG.info("scan "+(end-start)/1000L);

			start=System.currentTimeMillis();
			while (it.hasNext()) {
				Result next = it.next();
				Cell cell = next.getColumnLatestCell(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
				//System.out.println(cell.getTimestamp()+"---");
//				byte[] valueArray = CellUtil.cloneValue(cell);
//				UserId cellUserId = Util.byteToUserId(valueArray);
//				byte[] rowkey = it.next().getRow();
//				UserId rowkeyUserId = Util.byteToRowkey(rowkey);


//				public byte[] getValue(byte [] family, byte [] qualifier) {
//					Cell kv = getColumnLatestCell(family, qualifier);
//					if (kv == null) {
//						return null;
//					}
//					return CellUtil.cloneValue(kv);
//				}

				byte[] valueBytes = next.getValue(Bytes.toBytes(CF), Bytes.toBytes(USERID_QUALIFIER));
				UserId userId = Util.byteToUserId(valueBytes);

//				System.out.println("row---"+cellUserId.getUserId());
//				System.out.println("value---"+userId.getUserId());
//				assert cellUserId.getUserId().equals(userId.getUserId());



				userId.setTimetamp(cell.getTimestamp());
				ret.add(userId);

			}
			 end=System.currentTimeMillis();
			//LOG.info("pack time "+(end-start)/1000L);
			return ret;
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			if(scanner!=null){
				scanner.close();
			}
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}


		return ret;
	}





	public void createConnection(Configuration conf)  {

		try{
			if (connections == null) {
				connections  = ConnectionFactory.createConnection(conf);
			}
		}catch (Exception e){
			e.printStackTrace();
			if(connections!=null){
				close();
			}
		}

	}

	public void close()  {
		try{
			connections.close();
		}catch (Exception e){
			LOG.error(e.getMessage());
		}

	}

}
