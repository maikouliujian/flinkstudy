package hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

/**
 * Created by yangyibo
 * Date: 2019/6/21
 * Time: 下午4:07
 */
public class Util {


	/**
	 * userId 转换成bytes
	 * @param userId
	 * @return
	 */
	public static byte[] userIdToByte(com.yidian.blacklist.UserId userId){

		byte[] uerIdByte = Bytes.toBytes(userId.getUserId());
		byte[] statusByte = Bytes.toBytes(userId.getStatus());

		byte[] bytes=new byte[uerIdByte.length+statusByte.length];
		System.arraycopy(uerIdByte,0,bytes,0,uerIdByte.length);
		System.arraycopy(statusByte,0,bytes,uerIdByte.length,statusByte.length);

		return bytes;

	}



	/**
	 *
	 * @param bytes
	 * @return
	 */
	private static long bytesToLong(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(bytes);
		buffer.flip();//need flip
		return buffer.getLong();
	}

	/**
	 * bytes[] 转为 userid
	 * @param bytes
	 * @return
	 */
	public static com.yidian.blacklist.UserId byteToUserId(byte[] bytes){

		com.yidian.blacklist.UserId ret=new com.yidian.blacklist.UserId();

		byte[] userIds=new  byte[bytes.length-8];
		byte[] status = new byte[8];

		System.arraycopy(bytes,0,userIds,0,userIds.length);
		System.arraycopy(bytes,userIds.length,status,0,status.length);

		ret.setUserId(new String(userIds));
		ret.setStatus(bytesToLong(status));



		return  ret;

	}


	public byte[] getRow(long timestamp, com.yidian.blacklist.UserId userId) {

		byte[] bytes = Bytes.toBytes(userId.getUserId());
		long reverseTimestamp = Long.MAX_VALUE - timestamp;
		ByteBuffer buffer = ByteBuffer.allocate(bytes.length+ 8);
		buffer.putLong(reverseTimestamp);
		buffer.put(bytes);

		return buffer.array();

	}


	public static com.yidian.blacklist.UserId byteToRowkey(byte[] bytes){

		com.yidian.blacklist.UserId ret=new com.yidian.blacklist.UserId();

		byte[] userIds=new  byte[bytes.length-8];
		byte[] ts = new byte[8];

		System.arraycopy(bytes,0,ts,0,ts.length);
		System.arraycopy(bytes,ts.length,userIds,0,userIds.length);

//		System.arraycopy(bytes,0,userIds,0,userIds.length);
//		System.arraycopy(bytes,userIds.length,ts,0,ts.length);

		ret.setUserId(new String(userIds));
		ret.setTimetamp(bytesToLong(ts));

		return  ret;

	}



}
