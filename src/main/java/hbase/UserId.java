package hbase; /**
 * Created by yangyibo
 * Date: 2019/5/5
 * Time: 下午7:03
 */

import java.io.Serializable;

public class UserId implements Serializable {


	private String userId;
	private long timetamp;
	private long status;
	public static final long DELETE=-1;
	public  static final long ADD=1;

	public UserId(){

	}

	UserId(String userId){
		this.userId=userId;

	}
	public UserId(String userId,long timetamp){
		this.userId=userId;
		this.timetamp=timetamp;
		status=ADD;
	}

	public UserId(String userId,long timetamp,long status){
		this.userId=userId;
		this.timetamp=timetamp;
		this.status=status;
	}




	public long getStatus() {
		return status;
	}

	public void setStatus(long status) {
		this.status = status;
	}

	public long getTimetamp() {
		return timetamp;
	}

	public void setTimetamp(long timetamp) {
		this.timetamp = timetamp;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}





}
