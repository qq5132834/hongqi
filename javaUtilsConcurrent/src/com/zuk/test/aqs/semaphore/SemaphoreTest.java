package com.zuk.test.aqs.semaphore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/***
 * ���޵���Դʹ�ó���
 * 
 * һ�λ�ȡ������
 */
public class SemaphoreTest {

	private static int threadCount = 200;
	
	public static void main(String[] args) throws Exception{
		ExecutorService exec =  Executors.newCachedThreadPool();
		final Semaphore semaphore = new Semaphore(5); //����������5��������Դ����20����
		for(int i = 0; i < threadCount; i++){  //����200������
			final int threadNum = i;
			exec.execute(() -> {
				try {
					semaphore.acquire(3);  //��ȡ3�����
					test(threadNum);
					semaphore.release(3);  //�ͷ�3�����
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			});
		}
		System.out.println("finished.");
		exec.shutdown(); 
		//�����shutdown�����������Ͻ����е��߳����߳�ֹͣ�������ǵȴ�����ִ�е��߳�ִ�����֮��
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
