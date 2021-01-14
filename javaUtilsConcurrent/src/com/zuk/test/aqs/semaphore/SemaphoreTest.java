package com.zuk.test.aqs.semaphore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/***
 * 有限的资源使用场景
 * 
 * 一次获取多个许可
 */
public class SemaphoreTest {

	private static int threadCount = 200;
	
	public static void main(String[] args) throws Exception{
		ExecutorService exec =  Executors.newCachedThreadPool();
		final Semaphore semaphore = new Semaphore(5); //最大许可数是5（有限资源数是20个）
		for(int i = 0; i < threadCount; i++){  //发起200个请求
			final int threadNum = i;
			exec.execute(() -> {
				try {
					semaphore.acquire(3);  //获取3个许可
					test(threadNum);
					semaphore.release(3);  //释放3个许可
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			});
		}
		System.out.println("finished.");
		exec.shutdown(); 
		//这里的shutdown方法不会马上将所有的线程中线程停止掉，而是等待正在执行的线程执行完成之后
	}
	
	private static void test(int threadNum){
		System.out.println(threadNum);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
