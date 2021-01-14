package hbase;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangyibo
 * Date: 2019/1/14
 * Time: 下午7:20
 */

public class NamedThreadFactoryService implements ThreadFactory {

	private static AtomicInteger tag = new AtomicInteger(0);
	private String threadName;
	NamedThreadFactoryService(String threadName){
		this.threadName=threadName;
	}
	@Override
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r);
		thread.setName(threadName+"-thread-"+tag.getAndIncrement());
		return thread;
	}

}