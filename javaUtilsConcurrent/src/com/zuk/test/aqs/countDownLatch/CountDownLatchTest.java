package com.zuk.test.aqs.countDownLatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/***
 * 计数器的使用场景
 *
 */
public class CountDownLatchTest {

	private static int threadCount = 200;
	
	public static void main(String[] args) throws Exception{
		ExecutorService exec =  Executors.newCachedThreadPool();
		final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
		for(int i = 0; i < threadCount; i++){  //发起200个请求
			final int threadNum = i;
			exec.execute(() -> {
				try {
					test(threadNum);
				} catch (Exception e) {
					e.printStackTrace();
				} finally{
					countDownLatch.countDown(); //计数器递减
				}
			});
		}
		countDownLatch.await(); //等计数器减到0的时候才开始执行下面的“finished.”
		System.out.println("finished.");
		exec.shutdown(); 
		//这里的shutdown方法不会马上将所有的线程中线程停止掉，而是等待正在执行的线程执行完成之后
	}
	
	
	private static void test(int threadNum){
		System.out.println(threadNum);
	}
	
}
