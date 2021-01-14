package com.zuk.test.aqs.countDownLatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/***
 * �ȴ���ʱʱ��
 */
public class CountDownLatchTest2 {

	private static int threadCount = 200;
	
	public static void main(String[] args) throws Exception{
		ExecutorService exec =  Executors.newCachedThreadPool();
		final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
		for(int i = 0; i < threadCount; i++){  //����200������
			final int threadNum = i;
			exec.execute(() -> {
				try {
					test(threadNum);
				} catch (Exception e) {
					e.printStackTrace();
				} finally{
					countDownLatch.countDown(); //�������ݼ�
				}
			});
		}
		countDownLatch.await(10, TimeUnit.MILLISECONDS); //�ȴ�10����
		System.out.println("finished.");
		exec.shutdown(); 
		//�����shutdown�����������Ͻ����е��߳����߳�ֹͣ�������ǵȴ�����ִ�е��߳�ִ�����֮��
	}
	
	
	private static void test(int threadNum){
		try {
			Thread.sleep(100); //ֹͣ100����
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		System.out.println(threadNum);
	}
	
}
