package com.zuk.test.aqs.cyclicbarrier;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CyclicBarrierTest {

	private static CyclicBarrier cyclicBarrier = new CyclicBarrier(5);
	
	public static void main(String[] args) throws Exception {
		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i = 0; i < 20; i++) {
			final int threadNum = i;
			Thread.sleep(1000);
			exec.execute(() -> {
				race(threadNum);
			});
		}
	}
	
	private static void race(int threadNum){
		try {
			Thread.sleep(1000);
			System.out.println(threadNum + " is ready.");
			cyclicBarrier.await();
			System.out.println(threadNum + " continue.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
